"""Anti-spam / state-machine layer for emitted signals.

Responsibilities:
- Per-symbol cooldown
- Duplicate-state suppression (same reasons + similar score in short time)
- Global rate limiting (no more than N signals per minute system-wide)
- Bridging WATCH/ARMED/COOLDOWN transitions
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from core.models import SymbolState

log = logging.getLogger("anti_spam")


@dataclass(slots=True)
class _Recent:
    ts_ms: int
    score: float
    tag_signature: str


class AntiSpam:
    def __init__(
        self,
        *,
        cooldown_sec: int = 1800,
        watch_ttl_sec: int = 900,
        duplicate_window_sec: int = 600,
        score_eps: float = 4.0,
        global_per_minute_cap: int = 4,
    ) -> None:
        self.cooldown_sec = cooldown_sec
        self.watch_ttl_sec = watch_ttl_sec
        self.duplicate_window_sec = duplicate_window_sec
        self.score_eps = score_eps
        self.global_per_minute_cap = global_per_minute_cap

        self._last_fire_ms: dict[str, int] = {}
        self._recent: dict[str, _Recent] = {}
        self._global: deque[int] = deque(maxlen=512)

    def _now(self) -> int:
        return int(time.time() * 1000)

    def can_fire(self, symbol: str, confidence: float, tags: list[str]) -> tuple[bool, str | None]:
        now_ms = self._now()
        # Per-symbol cooldown
        last = self._last_fire_ms.get(symbol, 0)
        if now_ms - last < self.cooldown_sec * 1000:
            return False, "cooldown"

        # Global rate cap (rolling 60s)
        cutoff = now_ms - 60_000
        while self._global and self._global[0] < cutoff:
            self._global.popleft()
        if len(self._global) >= self.global_per_minute_cap:
            return False, "global_rate_limit"

        # Duplicate-state suppression: same symbol + tag signature inside window
        sig = ",".join(sorted(tags))
        recent = self._recent.get(symbol)
        if recent and (now_ms - recent.ts_ms) < self.duplicate_window_sec * 1000:
            if recent.tag_signature == sig and abs(confidence - recent.score) < self.score_eps:
                return False, "duplicate"

        return True, None

    def record_fire(self, symbol: str, confidence: float, tags: list[str]) -> None:
        now_ms = self._now()
        self._last_fire_ms[symbol] = now_ms
        self._global.append(now_ms)
        self._recent[symbol] = _Recent(
            ts_ms=now_ms,
            score=confidence,
            tag_signature=",".join(sorted(tags)),
        )

    def cooldown_remaining_sec(self, symbol: str) -> int:
        last = self._last_fire_ms.get(symbol)
        if not last:
            return 0
        remaining = self.cooldown_sec - int((self._now() - last) / 1000)
        return max(0, remaining)

    def watch_expired(self, watch_started_ms: int) -> bool:
        return (self._now() - watch_started_ms) > self.watch_ttl_sec * 1000

    def transition_after_fire(self) -> SymbolState:
        return SymbolState.COOLDOWN

    def to_dict(self) -> dict[str, Any]:
        return {
            "active_cooldowns": {
                s: self.cooldown_remaining_sec(s)
                for s in self._last_fire_ms
                if self.cooldown_remaining_sec(s) > 0
            },
            "recent_global_signals_60s": len(self._global),
        }
