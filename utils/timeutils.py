"""Time helpers — kept ultra-light, no pendulum/arrow."""

from __future__ import annotations

import time
from datetime import UTC, datetime


def now_ms() -> int:
    """Current epoch milliseconds (UTC)."""
    return int(time.time() * 1000)


def now_s() -> float:
    return time.time()


def to_iso(ms: int | float) -> str:
    """Convert ms epoch -> ISO8601 string (UTC)."""
    if ms > 1e12:
        seconds = ms / 1000.0
    else:
        seconds = float(ms)
    return datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_minute(ts_ms: int, minutes: int = 1) -> int:
    """Floor an ms timestamp to the previous N-minute mark."""
    step = minutes * 60_000
    return (ts_ms // step) * step
