"""Exhaustion / reversal scorer.

Runs only on symbols already in WATCH mode. Each signal contributes a small
piece toward an aggregate score in [0, 100]. The scorer also returns the set
of reasons that materially contributed, so we can build a high-quality
Telegram message and tag setups for analytics.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.state import SymbolStateData
from utils import clamp, linear_score

from . import features, indicators
from .microstructure import (
    blowoff_top,
    buyer_exhaustion,
    climax_volume,
    cvd_divergence,
    delta_window,
    liquidity_grab,
    momentum_slowdown,
    oi_flat_after_pump,
)
from .orderflow import aggressive_sell_burst

DEFAULT_WEIGHTS: dict[str, float] = {
    "upper_wicks": 0.13,
    "failed_breakout": 0.12,
    "rejection_candle": 0.10,
    "liquidity_grab": 0.10,
    "cvd_divergence": 0.10,
    "delta_flip": 0.08,
    "climax_volume": 0.10,
    "oi_flat": 0.10,
    "vol_compression": 0.07,
    "rsi_divergence": 0.05,
    "vwap_deviation": 0.05,
}


@dataclass(slots=True)
class ExhaustionResult:
    score: float                # 0..100
    reasons: list[str]
    tags: list[str]             # discrete setup labels
    debug: dict[str, float]


class ExhaustionScorer:
    def __init__(self, weights: dict[str, float] | None = None,
                 min_confirmations: int = 3) -> None:
        self._weights = {**DEFAULT_WEIGHTS, **(weights or {})}
        self._min_confirmations = min_confirmations

    def evaluate(self, state: SymbolStateData) -> ExhaustionResult:
        if len(state.klines) < 25:
            return ExhaustionResult(0.0, [], [], {})

        contributions: list[tuple[str, str, float, float]] = []
        # tuple: (component_key, reason_str, score_value (0..1), weight)

        # Upper wicks: how dominated by upper wick are recent candles?
        uw = features.upper_wick_ratio(state, lookback=5)
        if uw > 0.35:
            s = linear_score(uw, 0.35, 0.75)
            contributions.append(("upper_wicks", "long upper wicks", s,
                                  self._weights["upper_wicks"]))

        # Failed breakout: broke prior swing then closed back below
        rb = features.recent_high_break(state, lookback=20)
        if rb["failed_breakout"]:
            contributions.append(("failed_breakout", "failed breakout",
                                  1.0, self._weights["failed_breakout"]))
        elif rb["broke"] and features.lower_high_sequence(state, lookback=5):
            contributions.append(("failed_breakout", "failed continuation",
                                  0.6, self._weights["failed_breakout"]))

        # Rejection candle
        if features.rejection_candle(state):
            contributions.append(("rejection_candle", "rejection candle",
                                  1.0, self._weights["rejection_candle"]))

        # Liquidity grab
        if liquidity_grab(state, lookback=20):
            contributions.append(("liquidity_grab", "liquidity grab",
                                  1.0, self._weights["liquidity_grab"]))

        # CVD divergence
        if cvd_divergence(state):
            contributions.append(("cvd_divergence", "CVD divergence",
                                  1.0, self._weights["cvd_divergence"]))

        # Delta flip: prior was bullish, current is bearish
        d_now = delta_window(state, window_sec=60)
        d_prev = delta_window(state, window_sec=300)
        if d_prev["ratio"] > 0.05 and d_now["ratio"] < -0.15:
            magnitude = clamp((d_prev["ratio"] - d_now["ratio"]) / 0.6, 0.4, 1.0)
            contributions.append(("delta_flip", "aggressive sell activity",
                                  magnitude, self._weights["delta_flip"]))
        elif aggressive_sell_burst(state, window_sec=120):
            contributions.append(("delta_flip", "aggressive sellers active",
                                  0.7, self._weights["delta_flip"]))

        # Climax / blowoff
        if blowoff_top(state):
            contributions.append(("climax_volume", "blowoff top",
                                  1.0, self._weights["climax_volume"]))
        elif climax_volume(state):
            contributions.append(("climax_volume", "volume climax",
                                  0.85, self._weights["climax_volume"]))

        # OI behavior
        if oi_flat_after_pump(state):
            contributions.append(("oi_flat", "OI flattening vs price",
                                  0.9, self._weights["oi_flat"]))

        # Volatility compression after expansion
        vcr = features.vol_compression_ratio(state)
        if 0 < vcr < 0.6:
            s = linear_score(1.0 - vcr, 0.4, 0.85)
            contributions.append(("vol_compression", "volatility compression",
                                  s, self._weights["vol_compression"]))

        # RSI divergence (secondary)
        if indicators.rsi_bearish_divergence(state, lookback=20):
            contributions.append(("rsi_divergence", "RSI divergence",
                                  1.0, self._weights["rsi_divergence"]))

        # VWAP deviation (secondary)
        vdev = indicators.vwap_deviation_pct(state, lookback=60)
        if vdev > 3.0:
            s = linear_score(vdev, 3.0, 9.0)
            contributions.append(("vwap_deviation", f"VWAP deviation +{vdev:.1f}%",
                                  s, self._weights["vwap_deviation"]))

        # Buyer exhaustion / momentum slowdown (treated as extra strength
        # on the delta/climax components rather than separate weights)
        bonus_signals = 0
        if buyer_exhaustion(state):
            bonus_signals += 1
        if momentum_slowdown(state):
            bonus_signals += 1

        if not contributions:
            return ExhaustionResult(0.0, [], [], {})

        # Weighted aggregation
        total_w = sum(w for *_, w in contributions)
        weighted = sum(s * w for *_, s, w in contributions) / max(total_w, 1e-9)

        # Boost score for cross-confirmation
        coverage = len({key for key, _, _, _ in contributions}) / len(self._weights)
        boost = 1.0 + 0.15 * coverage
        score = clamp(weighted * boost * 100.0, 0.0, 100.0)
        if bonus_signals:
            score = clamp(score + bonus_signals * 2.0, 0.0, 100.0)

        # If fewer than the minimum number of confirmations, cap the score so
        # we never fire low-coverage signals.
        unique_keys = len({k for k, *_ in contributions})
        if unique_keys < self._min_confirmations:
            score = min(score, 49.9)

        reasons = [reason for _, reason, _, _ in contributions]
        tags = [key for key, *_ in contributions]
        debug = {k: round(s, 3) for k, _, s, _ in contributions}
        debug["coverage"] = round(coverage, 3)
        debug["bonus"] = float(bonus_signals)

        return ExhaustionResult(score=score, reasons=reasons, tags=tags, debug=debug)
