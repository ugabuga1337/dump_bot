"""Engine entry point.

Run with::

    python -m main

It will:
- load config from .env / config/default.yaml
- bootstrap the engine
- handle SIGINT / SIGTERM cleanly
- attach a watchdog for the websocket fleet
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:  # pragma: no cover
    pass

from config import get_settings
from core.engine import Engine
from core.watchdog import Watchdog
from utils import setup_logging

log = logging.getLogger("main")


async def amain() -> int:
    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_json)
    log.info("dump_bot_main_start",
             extra={"service": os.environ.get("SERVICE", "engine")})

    engine = Engine(settings)
    watchdog = Watchdog(engine)
    await engine.start()
    watchdog.start()

    stop_event: asyncio.Event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        log.info("signal_received_stopping")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        await stop_event.wait()
    finally:
        await watchdog.stop()
        await engine.shutdown()
    return 0


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())
