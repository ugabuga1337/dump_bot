"""Periodic REST gainer scan that drives dynamic WS subscriptions.

Replaces the previous "subscribe to the whole universe" approach: a single
REST tickers query per cycle, deltas vs the previous snapshot, top gainers
become the active WS set.

The scanner does not own the WS — it just produces the active symbol list.
The engine owns subscription wiring.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from config import GainerConfig
from connectors import BinanceFuturesREST

log = logging.getLogger("gainer")


class GainerScanner:
    def __init__(
        self,
        cfg: GainerConfig,
        rest: BinanceFuturesREST,
        *,
        universe: Iterable[str] | None = None,
    ) -> None:
        self._cfg = cfg
        self._rest = rest
        # Pre-filtered candidate pool (junk filter) — if None, all returned
        # symbols are considered.
        self._universe: set[str] | None = set(universe) if universe is not None else None
        self._snapshots: dict[str, float] = {}
        self._miss_counter: dict[str, int] = {}
        self._last_active: set[str] = set()
        self._last_cycle_candidates: int = 0
        self._cycles: int = 0

    # ---------- configuration ----------

    def set_universe(self, universe: Iterable[str]) -> None:
        """Refresh the junk-filtered candidate pool."""
        self._universe = set(universe)
        # Drop snapshots / counters for symbols no longer in the pool.
        self._snapshots = {s: p for s, p in self._snapshots.items() if s in self._universe}
        self._miss_counter = {
            s: c for s, c in self._miss_counter.items() if s in self._universe
        }

    # ---------- introspection ----------

    @property
    def active(self) -> set[str]:
        return set(self._last_active)

    def stats(self) -> dict[str, Any]:
        return {
            "cycles": self._cycles,
            "snapshots": len(self._snapshots),
            "active": len(self._last_active),
            "candidates_last_cycle": self._last_cycle_candidates,
        }

    # ---------- core scan ----------

    async def scan(self, *, protected: Iterable[str] = ()) -> list[str]:
        """Run one REST scan cycle and return the next active symbol list.

        ``protected`` lists symbols the caller wants kept active regardless of
        delta (e.g. those currently in WATCH/COOLDOWN).
        """
        try:
            tickers = await self._rest.ticker_24h()
        except Exception as exc:  # noqa: BLE001 — never let scanner crash the loop
            log.warning("gainer_scan_failed", extra={"err": repr(exc)})
            return sorted(self._last_active | set(protected))

        self._cycles += 1
        new_snapshots: dict[str, float] = {}
        deltas: list[tuple[str, float]] = []

        for t in tickers:
            symbol = str(t.get("symbol", "")).upper()
            if not symbol:
                continue
            if self._universe is not None and symbol not in self._universe:
                continue
            try:
                price = float(t.get("lastPrice") or 0.0)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            new_snapshots[symbol] = price

            prev = self._snapshots.get(symbol)
            if prev is None or prev <= 0:
                continue
            delta_pct = (price - prev) / prev * 100.0
            if delta_pct >= self._cfg.min_pct:
                deltas.append((symbol, delta_pct))

        deltas.sort(key=lambda r: r[1], reverse=True)
        candidates = [s for s, _ in deltas[: self._cfg.top_n]]
        candidate_set = set(candidates)
        self._last_cycle_candidates = len(deltas)

        # Bump miss counters for previously-active symbols not in candidates.
        for sym in list(self._last_active):
            if sym in candidate_set:
                self._miss_counter[sym] = 0
            else:
                self._miss_counter[sym] = self._miss_counter.get(sym, 0) + 1

        # Reset miss counter for fresh candidates.
        for sym in candidates:
            self._miss_counter[sym] = 0

        protected_set = {s.upper() for s in protected}

        next_active: set[str] = set(candidates) | protected_set
        # Carry over still-fresh previously-active symbols.
        for sym in self._last_active:
            if sym in protected_set:
                next_active.add(sym)
                continue
            if self._miss_counter.get(sym, 0) < self._cfg.drop_after_cycles:
                next_active.add(sym)

        # Clean up counters for symbols no longer active.
        self._miss_counter = {s: c for s, c in self._miss_counter.items() if s in next_active}

        self._snapshots = new_snapshots
        self._last_active = next_active

        log.info(
            "gainer_scan",
            extra={
                "cycle": self._cycles,
                "candidates": len(deltas),
                "top": len(candidates),
                "active": len(next_active),
                "protected": len(protected_set),
            },
        )
        return sorted(next_active)
