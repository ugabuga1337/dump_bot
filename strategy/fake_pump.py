"""Fake pump / manipulation detector.

Computes a 0-100 ``fake_pump_score``. A high score *penalizes* confidence,
because manipulated spikes tend to revert violently in either direction —
sometimes shooting higher before any reversal.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.state import SymbolStateData
from utils import clamp, linear_score

from . import features
from .orderflow import delta_window

DEFAULT_WEIGHTS: dict[str, float] = {
    "thin_liquidity": 0.30,
    "wick_dominated": 0.25,
    "no_oi_growth": 0.20,
    "one_sided_trades": 0.15,
    "cooldown_burst": 0.10,
}


@dataclass(slots=True)
class FakePumpResult:
    score: float
    flags: list[str]


class FakePumpDetector:
    def __init__(self, weights: dict[str, float] | None = None) -> None:
        self._weights = {**DEFAULT_WEIGHTS, **(weights or {})}

    def evaluate(self, state: SymbolStateData) -> FakePumpResult:
        contributions: list[tuple[str, str, float, float]] = []

        # Thin liquidity proxy: very few trades but very high price move.
        if len(state.trades) > 10 and len(state.klines) > 10:
            recent = list(state.trades)[-200:]
            notional = sum(t.qty * t.price for t in recent)
            ret_5m = state.recent_return_pct(5)
            if ret_5m > 5.0 and notional < 50_000:
                score = linear_score(ret_5m, 5.0, 15.0)
                contributions.append(("thin_liquidity", "thin liquidity", score,
                                      self._weights["thin_liquidity"]))

        # Wick dominated
        uw = features.upper_wick_ratio(state, lookback=5)
        if uw > 0.6:
            score = linear_score(uw, 0.6, 0.85)
            contributions.append(("wick_dominated", "wick dominated", score,
                                  self._weights["wick_dominated"]))

        # No OI growth: big price move with zero / tiny OI change
        ret_5m = state.recent_return_pct(5)
        oi_5m = state.oi_change_pct(5)
        if ret_5m > 4.0 and oi_5m < 0.5:
            score = linear_score(ret_5m / max(0.5, oi_5m + 0.5), 4.0, 12.0)
            contributions.append(("no_oi_growth", "no OI growth", score,
                                  self._weights["no_oi_growth"]))

        # One-sided trades (only one cluster of aggressive buyers, no follow through)
        d_long = delta_window(state, window_sec=600)
        d_short = delta_window(state, window_sec=60)
        if d_long["total"] > 0 and d_short["total"] > 0:
            if d_long["ratio"] > 0.5 and d_short["total"] < d_long["total"] * 0.05:
                contributions.append(("one_sided_trades", "single-side trade burst",
                                      0.8, self._weights["one_sided_trades"]))

        # Cooldown burst — a sudden spike right after recent volatility, often
        # a continuation trap. Detected via volatility expansion ratio.
        vcr = features.vol_compression_ratio(state)
        if vcr > 2.5:
            score = linear_score(vcr, 2.5, 5.0)
            contributions.append(("cooldown_burst", "post-quiet burst", score,
                                  self._weights["cooldown_burst"]))

        if not contributions:
            return FakePumpResult(0.0, [])

        total_w = sum(w for *_, w in contributions)
        weighted = sum(s * w for *_, s, w in contributions) / max(total_w, 1e-9)
        score = clamp(weighted * 100.0, 0.0, 100.0)
        flags = [reason for _, reason, _, _ in contributions]
        return FakePumpResult(score=score, flags=flags)
