"""Paper-analysis outcome tracker.

After each signal, we watch the next ``track_seconds`` of price action to
compute:

- max_favorable_pct  -> deepest dump in our favor (short side)
- max_adverse_pct    -> worst spike against us
- reversal_speed_sec -> time from signal until favorable threshold first crossed
- final_pct          -> close-to-signal price diff at track end
- hit_tp1 / hit_tp2 / hit_sl / invalidated -> binary flags

Outcomes are persisted in the ``signal_outcomes`` table and used by analytics.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from config import OutcomeConfig
from storage import Repository
from utils import now_ms

log = logging.getLogger("outcome")


@dataclass(slots=True)
class _Tracked:
    signal_id: int
    symbol: str
    entry_price: float
    suggested_sl: float
    suggested_tp1: float
    suggested_tp2: float
    started_ms: int
    tracked_until_ms: int
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
    reversal_speed_sec: float | None = None
    hit_tp1: bool = False
    hit_tp2: bool = False
    hit_sl: bool = False
    invalidated: bool = False
    last_price: float = 0.0
    extras: dict[str, Any] = field(default_factory=dict)


class OutcomeTracker:
    def __init__(self, cfg: OutcomeConfig, repo: Repository) -> None:
        self._cfg = cfg
        self._repo = repo
        self._tracked: dict[int, _Tracked] = {}
        self._lock = asyncio.Lock()

    async def restore_open(self) -> None:
        """Re-load open outcomes from DB on startup."""
        rows = await self._repo.open_outcomes()
        async with self._lock:
            for r in rows:
                created = int(r["created_ms"])
                if now_ms() - created > self._cfg.track_seconds * 1000 + 5_000:
                    continue
                self._tracked[r["id"]] = _Tracked(
                    signal_id=int(r["id"]),
                    symbol=r["symbol"],
                    entry_price=float(r["price"]),
                    suggested_sl=float(r["suggested_sl"] or 0.0),
                    suggested_tp1=float(r["suggested_tp1"] or 0.0),
                    suggested_tp2=float(r["suggested_tp2"] or 0.0),
                    started_ms=created,
                    tracked_until_ms=created + self._cfg.track_seconds * 1000,
                    last_price=float(r["price"]),
                )

    async def add(
        self,
        signal_id: int,
        *,
        symbol: str,
        entry_price: float,
        suggested_sl: float,
        suggested_tp1: float,
        suggested_tp2: float,
    ) -> None:
        async with self._lock:
            self._tracked[signal_id] = _Tracked(
                signal_id=signal_id,
                symbol=symbol,
                entry_price=entry_price,
                suggested_sl=suggested_sl,
                suggested_tp1=suggested_tp1,
                suggested_tp2=suggested_tp2,
                started_ms=now_ms(),
                tracked_until_ms=now_ms() + self._cfg.track_seconds * 1000,
                last_price=entry_price,
            )

    async def on_price(self, symbol: str, price: float, ts_ms: int) -> None:
        """Called on each price update for any tracked symbol."""
        to_close: list[int] = []
        async with self._lock:
            for sid, t in self._tracked.items():
                if t.symbol != symbol:
                    continue
                t.last_price = price
                # signed % move from entry (positive = price moved up vs entry; bad for short)
                up_pct = (price - t.entry_price) / t.entry_price * 100.0 if t.entry_price else 0.0
                # favorable for short is when price drops
                fav = -up_pct
                if fav > t.max_favorable_pct:
                    t.max_favorable_pct = fav
                    if t.reversal_speed_sec is None and fav >= 0.5:
                        t.reversal_speed_sec = max(0.001, (ts_ms - t.started_ms) / 1000.0)
                if up_pct > t.max_adverse_pct:
                    t.max_adverse_pct = up_pct
                if t.suggested_sl and price >= t.suggested_sl:
                    t.hit_sl = True
                if t.suggested_tp1 and price <= t.suggested_tp1:
                    t.hit_tp1 = True
                if t.suggested_tp2 and price <= t.suggested_tp2:
                    t.hit_tp2 = True
                if up_pct >= self._cfg.invalidation_pct:
                    t.invalidated = True

                if ts_ms >= t.tracked_until_ms or t.hit_sl or t.hit_tp2 or t.invalidated:
                    to_close.append(sid)

        for sid in to_close:
            await self._finalize(sid)

    async def _finalize(self, signal_id: int) -> None:
        async with self._lock:
            t = self._tracked.pop(signal_id, None)
        if not t:
            return
        final_pct = (t.last_price - t.entry_price) / t.entry_price * 100.0 if t.entry_price else 0.0
        is_win = (t.hit_tp1 or t.max_favorable_pct >= 1.5) and not t.invalidated
        row = {
            "tracked_until_ms": t.tracked_until_ms,
            "max_favorable_pct": round(t.max_favorable_pct, 3),
            "max_adverse_pct": round(t.max_adverse_pct, 3),
            "reversal_speed_sec": round(t.reversal_speed_sec, 2) if t.reversal_speed_sec else None,
            "final_pct": round(final_pct, 3),
            "hit_tp1": int(t.hit_tp1),
            "hit_tp2": int(t.hit_tp2),
            "hit_sl": int(t.hit_sl),
            "invalidated": int(t.invalidated),
            "is_win": int(is_win),
            "closed_ms": now_ms(),
        }
        try:
            await self._repo.upsert_outcome(t.signal_id, row)
        except Exception as exc:  # noqa: BLE001
            log.warning("outcome_persist_failed",
                        extra={"signal_id": t.signal_id, "err": repr(exc)})
        log.info("outcome",
                 extra={"signal_id": t.signal_id, "symbol": t.symbol, "is_win": int(is_win),
                        "fav_pct": row["max_favorable_pct"], "adv_pct": row["max_adverse_pct"]})

    def tracked_symbols(self) -> set[str]:
        return {t.symbol for t in self._tracked.values()}

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "signal_id": t.signal_id,
                "symbol": t.symbol,
                "entry_price": t.entry_price,
                "fav_pct": round(t.max_favorable_pct, 2),
                "adv_pct": round(t.max_adverse_pct, 2),
                "started_ms": t.started_ms,
                "tracked_until_ms": t.tracked_until_ms,
            }
            for t in self._tracked.values()
        ]
