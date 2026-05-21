"""Binance Futures WebSocket multiplexed client.

Subscribes to per-symbol combined streams (aggTrade, kline_1m, markPrice@1s)
on a single websocket connection (or sharded if the universe grows past a
practical URL length). Auto-reconnects with exponential backoff and exposes a
push-style ``on_message`` callback.

Designed to be cheap: one async task per connection, decoded payloads
delivered into in-memory state — no JSON copying beyond ``orjson.loads``.
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
except ImportError:  # pragma: no cover
    import json as _json_std

    def _loads(b: bytes | str) -> Any:
        return _json_std.loads(b if isinstance(b, str) else b.decode())


log = logging.getLogger("binance.ws")


# Conservative URL-length budget. Each stream is ~25 chars; 200 streams ≈ 5KB.
MAX_STREAMS_PER_CONN = 180


class BinanceWSClient:
    """One websocket connection consuming a set of streams.

    Hardened against the typical failure modes:
    - connection_closed / DNS / timeouts -> reconnect with jitter
    - silent connection (no messages) -> ping/pong (websockets handles default)
    - explicit Binance 24h drop -> reconnect immediately
    """

    def __init__(
        self,
        ws_base: str,
        streams: Sequence[str],
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        *,
        name: str = "ws",
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
    ) -> None:
        if not streams:
            raise ValueError("at least one stream required")
        self._ws_base = ws_base.rstrip("/")
        self._streams = list(streams)
        self._on_message = on_message
        self._name = name
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout

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

    def _build_url(self) -> str:
        joined = "/".join(self._streams)
        return f"{self._ws_base}/stream?streams={joined}"

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping.is_set():
            url = self._build_url()
            try:
                async with websockets.connect(
                    url,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                    open_timeout=20.0,
                    close_timeout=5.0,
                    max_size=2**20,  # 1MB max frame is plenty for futures public
                    compression=None,
                ) as ws:
                    self._connected.set()
                    backoff = 1.0
                    log.info("ws_connected",
                             extra={"name": self._name, "streams": len(self._streams)})
                    async for raw in ws:
                        if self._stopping.is_set():
                            break
                        try:
                            payload = _loads(raw)
                        except Exception:  # noqa: BLE001 — robust to corrupt frames
                            log.warning("ws_decode_failed", extra={"name": self._name})
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
                self._connected.clear()

            if self._stopping.is_set():
                return
            self._reconnects += 1
            # Exponential backoff with jitter; capped to keep tasks responsive.
            wait = min(30.0, backoff) + random.uniform(0.0, 0.5)
            await asyncio.sleep(wait)
            backoff = min(30.0, backoff * 2)

    async def _safe_dispatch(self, payload: dict[str, Any]) -> None:
        try:
            await self._on_message(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — never let a callback kill the loop
            log.error("ws_dispatch_error",
                      extra={"name": self._name, "err": repr(exc)}, exc_info=True)


class StreamManager:
    """Owns a fleet of BinanceWSClient connections, sharded by stream count.

    Re-shards transparently when ``update_streams`` is called with a new symbol
    set — typically on universe refresh.
    """

    def __init__(
        self,
        ws_base: str,
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._ws_base = ws_base
        self._on_message = on_message
        self._clients: list[BinanceWSClient] = []

    @property
    def clients(self) -> list[BinanceWSClient]:
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

        # Shard the list into chunks of MAX_STREAMS_PER_CONN.
        chunks = [
            list(streams)[i : i + MAX_STREAMS_PER_CONN]
            for i in range(0, len(streams), MAX_STREAMS_PER_CONN)
        ]
        for idx, chunk in enumerate(chunks):
            client = BinanceWSClient(
                self._ws_base, chunk, self._on_message, name=f"shard{idx}"
            )
            self._clients.append(client)
            client.start()
        log.info("stream_manager_started",
                 extra={"shards": len(chunks), "streams": len(streams)})

    def stats(self) -> list[dict[str, Any]]:
        return [c.stats() for c in self._clients]
