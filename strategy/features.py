"""Streaming feature extraction.

Pure functions over SymbolStateData. Everything here is O(window_size) — no
historical recomputation, no DataFrames, no allocations of per-call lists.
"""

from __future__ import annotations

from typing import Any

from core.state import SymbolStateData


def velocity(state: SymbolStateData, lookback: int = 5) -> float:
    """Average % move per minute over the last ``lookback`` closed klines."""
    if len(state.klines) < lookback + 1:
        return 0.0
    last = state.klines[-1].close
    ref = state.klines[-(lookback + 1)].close
    if ref == 0:
        return 0.0
    return (last - ref) / ref * 100.0 / lookback


def acceleration(state: SymbolStateData) -> float:
    """Simple 2nd-derivative proxy: recent velocity minus prior velocity."""
    v_recent = velocity(state, lookback=5)
    v_prior = velocity_offset(state, lookback=5, offset=5)
    return v_recent - v_prior


def velocity_offset(state: SymbolStateData, *, lookback: int, offset: int) -> float:
    """Velocity computed on a window shifted by ``offset`` candles back."""
    need = lookback + offset + 1
    if len(state.klines) < need:
        return 0.0
    last = state.klines[-(offset + 1)].close
    ref = state.klines[-(lookback + offset + 1)].close
    if ref == 0:
        return 0.0
    return (last - ref) / ref * 100.0 / lookback


def atr_pct(state: SymbolStateData, period: int = 14) -> float:
    if len(state.klines) < period + 1:
        return 0.0
    klines = list(state.klines)[-(period + 1):]
    tr_sum = 0.0
    for i in range(1, len(klines)):
        k = klines[i]
        prev = klines[i - 1]
        tr = max(k.high - k.low, abs(k.high - prev.close), abs(k.low - prev.close))
        tr_sum += tr
    atr = tr_sum / period
    last_close = klines[-1].close
    return (atr / last_close) * 100.0 if last_close else 0.0


def vol_compression_ratio(state: SymbolStateData) -> float:
    """High when recent vol is much LOWER than peak — i.e. compression after spike."""
    if len(state.highs_1m) < 30:
        return 0.0
    ranges = []
    closes = state.closes_1m.to_list()
    highs = state.highs_1m.to_list()
    lows = state.lows_1m.to_list()
    n = len(closes)
    for i in range(n):
        ranges.append(highs[i] - lows[i])
    recent = ranges[-5:]
    earlier = ranges[-30:-5]
    if not earlier or not recent:
        return 0.0
    mr = sum(recent) / len(recent)
    me = sum(earlier) / len(earlier)
    if me == 0:
        return 0.0
    # >1.0 means recent range bigger than baseline (expansion)
    # <1.0 means compression
    return mr / me


def upper_wick_ratio(state: SymbolStateData, lookback: int = 5) -> float:
    """Average ratio of upper wick to candle range over recent closed klines."""
    if len(state.klines) < lookback:
        return 0.0
    klines = [k for k in list(state.klines)[-lookback:] if (k.high - k.low) > 0]
    if not klines:
        return 0.0
    ratios = []
    for k in klines:
        rng = k.high - k.low
        body_top = max(k.open, k.close)
        upper = max(0.0, k.high - body_top)
        ratios.append(upper / rng if rng else 0.0)
    return sum(ratios) / len(ratios)


def lower_wick_ratio(state: SymbolStateData, lookback: int = 5) -> float:
    if len(state.klines) < lookback:
        return 0.0
    klines = [k for k in list(state.klines)[-lookback:] if (k.high - k.low) > 0]
    if not klines:
        return 0.0
    ratios = []
    for k in klines:
        rng = k.high - k.low
        body_low = min(k.open, k.close)
        lower = max(0.0, body_low - k.low)
        ratios.append(lower / rng if rng else 0.0)
    return sum(ratios) / len(ratios)


def recent_high_break(state: SymbolStateData, lookback: int = 20) -> dict[str, Any]:
    """Detect failed-breakout: did the most recent candle break the prior swing high
    then close back below it?"""
    if len(state.klines) < lookback + 1:
        return {"failed_breakout": False, "broke": False}
    klines = list(state.klines)
    last = klines[-1]
    prior = klines[-(lookback + 1):-1]
    prior_high = max(k.high for k in prior)
    broke = last.high > prior_high * 1.0005
    close_back_below = last.close < prior_high
    failed = broke and close_back_below
    return {"failed_breakout": failed, "broke": broke, "prior_high": prior_high}


def lower_high_sequence(state: SymbolStateData, lookback: int = 5) -> bool:
    if len(state.klines) < lookback:
        return False
    highs = [k.high for k in list(state.klines)[-lookback:]]
    descending = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i - 1])
    return descending >= max(2, lookback - 2)


def rejection_candle(state: SymbolStateData) -> bool:
    """Last candle has long upper wick + close < open (bearish rejection)."""
    if not state.klines:
        return False
    k = state.klines[-1]
    rng = k.high - k.low
    if rng <= 0:
        return False
    body = abs(k.close - k.open)
    upper = k.high - max(k.open, k.close)
    return (upper / rng) > 0.55 and k.close < k.open and body / rng < 0.35
