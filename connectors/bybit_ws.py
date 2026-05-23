"""Bybit v5 public WebSocket multiplexed client (USDT linear perpetuals).

Subscribes to per-symbol topics (``publicTrade.<SYM>``, ``kline.1.<SYM>``,
``tickers.<SYM>``) over the linear public stream. Auto-reconnects with
exponential backoff, sends Bybit's application-level pings every 20 s,
and exposes a push-style ``on_message`` callback.

One async task per connection; payloads are delivered into in-memory
state — no JSON copying beyond ``orjson.loads``.
"""

from __future__ import annotations

import asyncio
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

    def _dumps(o: Any) -> str:
        return _json.dumps(o).decode()
except ImportError:  # pragma: no cover
    import json as _json_std

    def _loads(b: bytes | str) -> Any:
        return _json_std.loads(b if isinstance(b, str) else b.decode())

    def _dumps(o: Any) -> str:
        return _json_std.dumps(o, separators=(",", ":"))


log = logging.getLogger("bybit.ws")


# Bybit linear public allows up to 200 topics per connection (per their docs).
# Stay well under that to leave headroom for ticker upgrades.
MAX_TOPICS_PER_CONN = 180

# A single subscribe frame can carry up to 10 topics.
MAX_TOPICS_PER_FRAME = 10

# Bybit drops idle connections after 30s without a ping.
PING_INTERVAL_SEC = 20.0

# Bybit's WS endpoint sits behind a CDN that silently drops connections
# with the default Python websockets handshake. Pretend to be a browser.
# Without these headers, the WS task gets stuck in ``websockets.connect``
# with no exception until ``open_timeout`` fires — and once enough other
# concurrent work is running in the engine, even that timeout doesn't
# trigger reliably. Sending a real-looking UA + Origin makes the
# handshake complete in <1s, matching what wscat / a browser would do.
_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin": "https://www.bybit.com",
}


class BybitWSClient:
    """One websocket connection consuming a set of Bybit topics.

    Hardened against the typical failure modes:
    - connection_closed / DNS / timeouts -> reconnect with jitter
    - silent connection (no messages) -> application ping every 20s
    - server disconnect (24h, idle) -> reconnect immediately
    """

    def __init__(
        self,
        ws_url: str,
        topics: Sequence[str],
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        *,
        name: str = "ws",
        ping_interval: float = PING_INTERVAL_SEC,
    ) -> None:
        if not topics:
            raise ValueError("at least one topic required")
        self._ws_url = ws_url
        self._topics = list(topics)
        self._on_message = on_message
        self._name = name
        self._ping_interval = ping_interval

        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._connected = asyncio.Event()
        self._last_msg_ts: float = 0.0
        self._msg_count: int = 0
        self._reconnects: int = 0

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

    @property
    def streams(self) -> list[str]:
        return list(self._topics)

    def stats(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "streams": len(self._topics),
            "messages": self._msg_count,
            "reconnects": self._reconnects,
            "connected": self.connected,
            "last_msg_ts": self._last_msg_ts,
        }

    # ---------- internals ----------

    async def _run(self) -> None:
        # Emit this BEFORE anything else so we can prove from logs that the
        # task is actually getting scheduled by the event loop. If you see
        # ``stream_manager_started`` but never ``ws_task_started`` for the
        # same shard, the loop is starving the task (something is hogging
        # the loop with sync work).
        log.info("ws_task_started",
                 extra={"name": self._name, "topics": len(self._topics),
                        "url": self._ws_url})
        backoff = 1.0
        while not self._stopping.is_set():
            ping_task: asyncio.Task | None = None
            try:
                log.info("ws_connecting", extra={"name": self._name})
                async with websockets.connect(
                    self._ws_url,
                    open_timeout=20.0,
                    close_timeout=5.0,
                    max_size=2**20,
                    compression=None,
                    # We send our own application-level pings; disable the
                    # protocol-level keepalive so Bybit doesn't see two
                    # competing heartbeats.
                    ping_interval=None,
                    # Browser-like handshake — see ``_BROWSER_HEADERS``.
                    additional_headers=_BROWSER_HEADERS,
                ) as ws:
                    self._connected.set()
                    backoff = 1.0
                    log.info("ws_connected",
                             extra={"name": self._name, "topics": len(self._topics)})

                    # Subscribe in chunks of MAX_TOPICS_PER_FRAME.
                    for chunk in _chunks(self._topics, MAX_TOPICS_PER_FRAME):
                        await ws.send(_dumps({"op": "subscribe", "args": list(chunk)}))

                    ping_task = asyncio.create_task(
                        self._ping_loop(ws), name=f"ws-ping-{self._name}"
                    )

                    async for raw in ws:
                        if self._stopping.is_set():
                            break
                        try:
                            payload = _loads(raw)
                        except Exception:  # noqa: BLE001 — robust to corrupt frames
                            log.warning("ws_decode_failed", extra={"name": self._name})
                            continue

                        # Control frames: subscribe ack / pong / errors.
                        if isinstance(payload, dict) and "topic" not in payload:
                            op = payload.get("op")
                            if op in ("subscribe", "pong", "auth", "ping"):
                                if op == "subscribe" and payload.get("success") is False:
                                    log.warning(
                                        "ws_subscribe_failed",
                                        extra={"name": self._name,
                                               "ret_msg": payload.get("ret_msg")},
                                    )
                                continue
                            # Unknown control message — log once but don't break.
                            log.debug("ws_ctrl_msg",
                                      extra={"name": self._name, "payload": payload})
                            continue

                        self._msg_count += 1
                        self._last_msg_ts = asyncio.get_event_loop().time()
                        await self._safe_dispatch(payload)
            except ConnectionClosed as exc:
                log.warning("ws_closed",
                            extra={"name": self._name, "code": exc.code,
                                   "reason": str(exc.reason)})
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("ws_error", extra={"name": self._name, "err": repr(exc)})
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except (asyncio.CancelledError, Exception):
                        pass
                self._connected.clear()

            if self._stopping.is_set():
                return
            self._reconnects += 1
            wait = min(30.0, backoff) + random.uniform(0.0, 0.5)
            await asyncio.sleep(wait)
            backoff = min(30.0, backoff * 2)

    async def _ping_loop(self, ws: Any) -> None:
        # Send the first ping immediately so Bybit's 30s idle-disconnect
        # never fires before our keepalive cadence kicks in — important on
        # cold start when subscribe acks and the first publish may take
        # several seconds.
        try:
            while True:
                try:
                    await ws.send(_dumps({"op": "ping"}))
                except ConnectionClosed:
                    return
                await asyncio.sleep(self._ping_interval)
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            log.debug("ws_ping_failed", extra={"name": self._name, "err": repr(exc)})

    async def _safe_dispatch(self, payload: dict[str, Any]) -> None:
        try:
            await self._on_message(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("ws_dispatch_error",
                      extra={"name": self._name, "err": repr(exc)}, exc_info=True)


def _chunks(seq: Sequence[str], n: int) -> list[list[str]]:
    return [list(seq[i : i + n]) for i in range(0, len(seq), n)]


class StreamManager:
    """Owns a fleet of BybitWSClient connections, sharded by topic count.

    Re-shards transparently when ``update_streams`` is called with a new
    topic set — typically on universe refresh.
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
        current = {s for c in self._clients for s in c.streams}
        if wanted == current and self._clients:
            return

        await self.stop()

        chunks = [
            list(streams)[i : i + MAX_TOPICS_PER_CONN]
            for i in range(0, len(streams), MAX_TOPICS_PER_CONN)
        ]
        for idx, chunk in enumerate(chunks):
            client = BybitWSClient(
                self._ws_url, chunk, self._on_message, name=f"shard{idx}"
            )
            self._clients.append(client)
            client.start()
        log.info("stream_manager_started",
                 extra={"shards": len(chunks), "topics": len(streams)})

    def stats(self) -> list[dict[str, Any]]:
        return [c.stats() for c in self._clients]
