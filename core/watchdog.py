"""Watchdog: periodic health checks of the engine.

This is intentionally light — a single asyncio task that:
- Pings the WebSocket message timestamps
- Restarts stream manager if a shard has been silent for too long
- Logs health stats
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

log = logging.getLogger("watchdog")


class Watchdog:
    def __init__(self, engine, *, ws_silence_sec: float = 90.0) -> None:
        self._engine = engine
        self._ws_silence_sec = ws_silence_sec
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name="watchdog")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        while not self._stopping.is_set():
            await asyncio.sleep(30)
            try:
                stats = self._engine.stream_manager.stats()
                now_loop = asyncio.get_event_loop().time()
                for s in stats:
                    last = s.get("last_msg_ts") or 0
                    if last and (now_loop - last) > self._ws_silence_sec:
                        log.warning("ws_silent_restart",
                                    extra={"shard": s.get("name"),
                                           "silence": now_loop - last})
                        # Force a stream rebuild — cheapest reliable fix.
                        try:
                            streams = self._engine._build_stream_list(self._engine.symbols)  # noqa: SLF001
                            await self._engine.stream_manager.update_streams(streams)
                        except Exception as exc:  # noqa: BLE001
                            log.warning("ws_rebuild_failed", extra={"err": repr(exc)})
                        break
                log.debug("watchdog_tick", extra={"stats": stats})
            except Exception as exc:  # noqa: BLE001
                log.warning("watchdog_error", extra={"err": repr(exc)})

    def health(self) -> dict[str, Any]:
        ws_stats = self._engine.stream_manager.stats() if self._engine else []
        ok = all(s.get("connected") for s in ws_stats)
        return {
            "ok": ok,
            "ws": ws_stats,
            "uptime_sec": int(time.time() - getattr(self._engine, "_started_at_s", time.time())),
        }
