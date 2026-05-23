"""Bybit linear perpetuals WebSocket multiplexed client.

Subscribes to per-symbol topics (publicTrade, kline.1, tickers) on a single
websocket connection (or sharded if the universe grows past Bybit's per-conn
arg cap). Auto-reconnects with exponential backoff and exposes a push-style
``on_message`` callback.

To keep the engine's message router stable, every Bybit frame is translated
into a Binance-compatible payload before being dispatched: the engine still
sees ``{"stream": "<sym>@aggTrade", "data": {"e": "aggTrade", ...}}`` and
friends. The translation lives here so adding/removing topics doesn't ripple
into ``core.engine``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

try:
    import orjson as _json

    def _loads(b: bytes | str) -> Any:
        if isinstance(b, str):
            b = b.encode()
        return _json.loads(b)

    def _dumps(obj: Any) -> str:
        return _json.dumps(obj).decode()
except ImportError:  # pragma: no cover
    import json as _json_std

    def _loads(b: bytes | str) -> Any:
        return _json_std.loads(b if isinstance(b, str) else b.decode())

    def _dumps(obj: Any) -> str:
        return _json_std.dumps(obj)


log = logging.getLogger("bybit.ws")


# Bybit accepts up to 10 args per subscribe op, and ~21,000 total args per
# connection. We cap conservatively at 180 args / shard (60 symbols × 3
# topics) — leaves plenty of headroom and keeps per-connection memory tiny.
MAX_STREAMS_PER_CONN = 180

# Subscribe in batches of N to stay under Bybit's 10-arg-per-message cap.
_SUBSCRIBE_BATCH = 10

# Browser-like headers. Some VPS providers route public WS traffic through
# transparent proxies that drop unfamiliar User-Agents; mimicking a normal
# browser session avoids the silent-connect failure mode we hit on Binance.
_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.bybit.com",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def _split_topic(topic: str) -> tuple[str, str]:
    """Return ``(kind, symbol)`` for a Bybit topic string.

    ``publicTrade.BTCUSDT`` -> ``("publicTrade", "BTCUSDT")``
    ``kline.1.BTCUSDT`` -> ``("kline", "BTCUSDT")``
    ``tickers.BTCUSDT`` -> ``("tickers", "BTCUSDT")``
    """
    parts = topic.split(".")
    if len(parts) < 2:
        return topic, ""
    return parts[0], parts[-1]


class BybitWSClient:
    """One websocket connection consuming a set of Bybit topics.

    Hardened against the typical failure modes:
    - connection_closed / DNS / timeouts -> reconnect with jitter
    - silent connection (no messages within ``stale_after`` s) -> force reconnect
    - app-level ping every ``ping_interval`` s to survive proxies that strip
      WebSocket control frames
    """

    def __init__(
        self,
        ws_url: str,
        streams: Sequence[str],
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        *,
        name: str = "ws",
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
        stale_after: float = 60.0,
    ) -> None:
        if not streams:
            raise ValueError("at least one stream required")
        self._ws_url = ws_url
        self._streams = list(streams)
        self._on_message = on_message
        self._name = name
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        # Force a reconnect if no frames arrive in this many seconds — guards
        # against the "silent connection" failure mode where the upgrade
        # succeeds but the exchange never pushes data.
        self._stale_after = stale_after

        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._connected = asyncio.Event()
        self._last_msg_ts: float = 0.0
        self._msg_count: int = 0
        self._reconnects: int = 0

        # Bybit ``tickers`` topic emits one snapshot then deltas; we need to
        # merge deltas into a per-symbol cache so the translated markPrice
        # payload always carries a full set of fields.
        self._ticker_cache: dict[str, dict[str, Any]] = {}

    # ---------- lifecycle ----------

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name=f"ws-{self._name}")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ---------- introspection ----------

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def stats(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "streams": len(self._streams),
            "messages": self._msg_count,
            "reconnects": self._reconnects,
            "connected": self.connected,
            "last_msg_ts": self._last_msg_ts,
        }

    # ---------- internals ----------

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping.is_set():
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                    open_timeout=20.0,
                    close_timeout=5.0,
                    max_size=2**20,
                    compression=None,
                    extra_headers=_DEFAULT_HEADERS,
                ) as ws:
                    self._connected.set()
                    backoff = 1.0
                    log.info(
                        "ws_connected",
                        extra={"name": self._name, "streams": len(self._streams)},
                    )
                    await self._subscribe(ws)
                    pinger = asyncio.create_task(
                        self._app_ping_loop(ws), name=f"ws-{self._name}-ping"
                    )
                    try:
                        await self._recv_loop(ws)
                    finally:
                        pinger.cancel()
                        with contextlib.suppress(BaseException):
                            await pinger
            except ConnectionClosed as exc:
                log.warning(
                    "ws_closed",
                    extra={"name": self._name, "code": exc.code,
                           "reason": str(exc.reason)},
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("ws_error", extra={"name": self._name, "err": repr(exc)})
            finally:
                self._connected.clear()

            if self._stopping.is_set():
                return
            self._reconnects += 1
            wait = min(30.0, backoff) + random.uniform(0.0, 0.5)
            await asyncio.sleep(wait)
            backoff = min(30.0, backoff * 2)

    async def _subscribe(self, ws: Any) -> None:
        for i in range(0, len(self._streams), _SUBSCRIBE_BATCH):
            batch = self._streams[i : i + _SUBSCRIBE_BATCH]
            await ws.send(_dumps({"op": "subscribe", "args": batch}))

    async def _app_ping_loop(self, ws: Any) -> None:
        """Send Bybit's app-level ping at ``ping_interval``.

        WebSocket protocol pings are usually enough, but Bybit recommends
        application pings to survive intermediaries that strip control frames.
        """
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(self._ping_interval)
                with contextlib.suppress(Exception):
                    await ws.send(_dumps({"op": "ping"}))
        except asyncio.CancelledError:
            return

    async def _recv_loop(self, ws: Any) -> None:
        while not self._stopping.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self._stale_after)
            except asyncio.TimeoutError:
                log.warning(
                    "ws_stale",
                    extra={"name": self._name, "stale_after": self._stale_after},
                )
                await ws.close(code=1000, reason="stale")
                return
            try:
                msg = _loads(raw)
            except Exception:  # noqa: BLE001
                log.warning("ws_decode_failed", extra={"name": self._name})
                continue
            for payload in self._translate(msg):
                self._msg_count += 1
                self._last_msg_ts = asyncio.get_event_loop().time()
                await self._safe_dispatch(payload)

    # ---------- translation ----------

    def _translate(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert one Bybit frame into zero-or-more Binance-style payloads."""
        if not isinstance(msg, dict):
            return []
        # Control / housekeeping frames.
        op = msg.get("op")
        if op in {"pong", "ping", "subscribe", "unsubscribe", "auth"}:
            if op == "subscribe" and msg.get("success") is False:
                log.warning(
                    "ws_subscribe_failed",
                    extra={"name": self._name, "ret": msg.get("ret_msg")},
                )
            return []
        # Subscribe replies sometimes arrive without an ``op`` key but with
        # ``ret_msg``/``success`` — same idea.
        if "topic" not in msg and ("ret_msg" in msg or "success" in msg):
            return []

        topic = msg.get("topic") or ""
        kind, symbol = _split_topic(topic)
        if not kind:
            return []
        ts = int(msg.get("ts") or 0)
        data = msg.get("data")

        if kind == "publicTrade" and isinstance(data, list):
            return [self._make_trade_payload(symbol, t, ts) for t in data if isinstance(t, dict)]

        if kind == "kline" and isinstance(data, list):
            return [self._make_kline_payload(symbol, k, ts) for k in data if isinstance(k, dict)]

        if kind == "tickers" and isinstance(data, dict):
            return self._make_mark_payload(symbol, data, msg.get("type"), ts)

        return []

    @staticmethod
    def _make_trade_payload(symbol: str, t: dict[str, Any], ts: int) -> dict[str, Any]:
        # Bybit ``S`` is the taker side; Binance ``m`` means "buyer is maker"
        # (i.e. the taker was the seller). Sell-side taker => buyer is maker.
        sym = (t.get("s") or symbol).upper()
        is_buyer_maker = (t.get("S") or "").lower() == "sell"
        return {
            "stream": f"{sym.lower()}@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": ts,
                "s": sym,
                "T": int(t.get("T") or ts),
                "p": t.get("p") or "0",
                "q": t.get("v") or "0",
                "m": is_buyer_maker,
            },
        }

    @staticmethod
    def _make_kline_payload(symbol: str, k: dict[str, Any], ts: int) -> dict[str, Any]:
        sym = symbol.upper()
        return {
            "stream": f"{sym.lower()}@kline_1m",
            "data": {
                "e": "kline",
                "E": int(k.get("timestamp") or ts),
                "s": sym,
                "k": {
                    "t": int(k.get("start") or 0),
                    "T": int(k.get("end") or 0),
                    "s": sym,
                    "i": str(k.get("interval") or "1"),
                    "o": k.get("open") or "0",
                    "h": k.get("high") or "0",
                    "l": k.get("low") or "0",
                    "c": k.get("close") or "0",
                    "v": k.get("volume") or "0",
                    "q": k.get("turnover") or "0",
                    # Bybit kline WS doesn't expose per-candle trade count or
                    # taker-buy splits — downstream features that read these
                    # treat the value as a soft signal, so 0 is acceptable.
                    "n": 0,
                    "V": "0",
                    "Q": "0",
                    "x": bool(k.get("confirm")),
                },
            },
        }

    def _make_mark_payload(
        self, symbol: str, data: dict[str, Any], msg_type: Any, ts: int
    ) -> list[dict[str, Any]]:
        sym = (data.get("symbol") or symbol).upper()
        cache = self._ticker_cache
        # Snapshot replaces; delta merges.
        if msg_type == "snapshot" or sym not in cache:
            cache[sym] = dict(data)
        else:
            cache[sym].update(data)
        cached = cache[sym]
        # Need at least the mark price to be useful to the strategy.
        if "markPrice" not in cached:
            return []
        try:
            mark = float(cached.get("markPrice") or 0.0)
            index = float(cached.get("indexPrice") or cached.get("markPrice") or 0.0)
            funding = float(cached.get("fundingRate") or 0.0)
            next_funding = int(cached.get("nextFundingTime") or 0)
        except (TypeError, ValueError):
            return []
        return [{
            "stream": f"{sym.lower()}@markPrice",
            "data": {
                "e": "markPriceUpdate",
                "E": ts,
                "s": sym,
                "p": f"{mark}",
                "i": f"{index}",
                "r": f"{funding}",
                "T": next_funding,
            },
        }]

    async def _safe_dispatch(self, payload: dict[str, Any]) -> None:
        try:
            await self._on_message(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — never let a callback kill the loop
            log.error("ws_dispatch_error",
                      extra={"name": self._name, "err": repr(exc)}, exc_info=True)


# Legacy alias — keeps ``from connectors.binance_ws import BinanceWSClient``
# working through the Bybit migration.
BinanceWSClient = BybitWSClient


class StreamManager:
    """Owns a fleet of :class:`BybitWSClient` connections, sharded by topic count.

    Re-shards transparently when ``update_streams`` is called with a new symbol
    set — typically on universe refresh.
    """

    def __init__(
        self,
        ws_url: str,
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._ws_url = ws_url
        self._on_message = on_message
        self._clients: list[BybitWSClient] = []

    @property
    def clients(self) -> list[BybitWSClient]:
        return list(self._clients)

    async def start(self, streams: Sequence[str]) -> None:
        await self.update_streams(streams)

    async def stop(self) -> None:
        for c in self._clients:
            await c.stop()
        self._clients.clear()

    async def update_streams(self, streams: Sequence[str]) -> None:
        wanted = set(streams)
        current = {s for c in self._clients for s in c._streams}  # noqa: SLF001
        if wanted == current and self._clients:
            return

        await self.stop()

        chunks = [
            list(streams)[i : i + MAX_STREAMS_PER_CONN]
            for i in range(0, len(streams), MAX_STREAMS_PER_CONN)
        ]
        for idx, chunk in enumerate(chunks):
            client = BybitWSClient(
                self._ws_url, chunk, self._on_message, name=f"shard{idx}"
            )
            self._clients.append(client)
            client.start()
        log.info("stream_manager_started",
                 extra={"shards": len(chunks), "streams": len(streams)})

    def stats(self) -> list[dict[str, Any]]:
        return [c.stats() for c in self._clients]
