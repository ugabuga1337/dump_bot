"""Microstructure / candle-structure features.

Convenience wrappers that aggregate features into named signals consumed by
exhaustion + fake pump scorers.
"""

from __future__ import annotations

from core.state import SymbolStateData

from . import features
from .orderflow import (
    aggressive_sell_burst,
    buyer_exhaustion,
    cvd_trend,
    delta_window,
    whale_print,
)


def liquidity_grab(state: SymbolStateData, lookback: int = 20) -> bool:
    """Sweep above a recent swing high followed by aggressive selling."""
    fb = features.recent_high_break(state, lookback=lookback)
    if not fb["broke"]:
        return False
    return aggressive_sell_burst(state, window_sec=60)


def climax_volume(state: SymbolStateData) -> bool:
    """Last 1-3 minutes drove an enormous spike but price barely advanced."""
    if len(state.volumes_1m) < 30:
        return False
    vols = state.volumes_1m.to_list()
    last3 = vols[-3:]
    base = vols[:-3]
    if not base:
        return False
    base_mean = sum(base) / len(base)
    if base_mean == 0:
        return False
    spike = (sum(last3) / 3.0) / base_mean
    # Big spike but velocity already slowing => climax
    v_recent = features.velocity(state, lookback=3)
    v_prior = features.velocity_offset(state, lookback=5, offset=3)
    return spike > 3.0 and v_recent < v_prior * 0.6


def blowoff_top(state: SymbolStateData) -> bool:
    """Parabolic move with a strong reversal candle and exhaustion signs."""
    if len(state.klines) < 12:
        return False
    five_min_ret = state.recent_return_pct(5)
    if five_min_ret < 5.0:
        return False
    rej = features.rejection_candle(state)
    sell_burst = aggressive_sell_burst(state, window_sec=90)
    return rej and sell_burst


def momentum_slowdown(state: SymbolStateData) -> bool:
    v_now = features.velocity(state, lookback=3)
    v_then = features.velocity_offset(state, lookback=3, offset=4)
    if v_then <= 0:
        return False
    return v_now < v_then * 0.4


def cvd_divergence(state: SymbolStateData) -> bool:
    """Price near recent highs while CVD slope is negative."""
    if len(state.klines) < 30:
        return False
    last = state.klines[-1].close
    high20 = max(k.high for k in list(state.klines)[-20:])
    near_high = last >= high20 * 0.997
    cvd = cvd_trend(state)
    return near_high and cvd["divergence"]


def oi_flat_after_pump(state: SymbolStateData) -> bool:
    """OI failed to keep growing while price kept pushing up."""
    if len(state.oi_history) < 6:
        return False
    pump_5m = state.recent_return_pct(5)
    if pump_5m < 3.0:
        return False
    oi_chg = state.oi_change_pct(5)
    # Price up >3% in 5m but OI did not also grow proportionally
    return oi_chg < pump_5m * 0.3


def wick_dominated(state: SymbolStateData, lookback: int = 5) -> bool:
    return features.upper_wick_ratio(state, lookback=lookback) > 0.55


__all__ = [
    "blowoff_top",
    "buyer_exhaustion",
    "climax_volume",
    "cvd_divergence",
    "delta_window",
    "liquidity_grab",
    "momentum_slowdown",
    "oi_flat_after_pump",
    "whale_print",
    "wick_dominated",
]
