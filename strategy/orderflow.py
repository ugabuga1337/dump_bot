"""Orderflow analytics on the rolling agg-trade buffer.

We don't try to be a proper L2 tape — futures aggTrades are pre-aggregated,
``is_buyer_maker=True`` means the aggressor was a SELLER (because the buyer
was sitting on the bid as maker). All deltas use this convention.
"""

from __future__ import annotations

from typing import Any

from core.state import SymbolStateData


def delta_window(state: SymbolStateData, window_sec: int) -> dict[str, float]:
    """Compute signed delta and total taker volume over a recent window.

    Returns:
        buy_vol, sell_vol, delta, total, delta_ratio (delta / total).
    """
    if not state.trades:
        return {"buy": 0.0, "sell": 0.0, "delta": 0.0, "total": 0.0, "ratio": 0.0}
    cutoff = state.trades[-1].ts_ms - window_sec * 1000
    buy = 0.0
    sell = 0.0
    for t in state.trades:
        if t.ts_ms < cutoff:
            continue
        notional = t.qty * t.price
        if t.is_buyer_maker:
            sell += notional
        else:
            buy += notional
    total = buy + sell
    delta = buy - sell
    ratio = (delta / total) if total else 0.0
    return {"buy": buy, "sell": sell, "delta": delta, "total": total, "ratio": ratio}


def cvd_trend(state: SymbolStateData, segments: int = 6) -> dict[str, Any]:
    """Sample the CVD at ``segments`` points over the trade buffer.

    Cheap proxy: split the buffer into N equal-time segments and report the
    direction of the last segment vs the first.
    """
    n = len(state.trades)
    if n < 30:
        return {"divergence": False, "slope_pct": 0.0}
    trades = list(state.trades)
    chunk = max(1, n // segments)
    samples: list[float] = []
    running = 0.0
    for i, t in enumerate(trades):
        notional = t.qty * t.price
        running += (-1.0 if t.is_buyer_maker else 1.0) * notional
        if (i + 1) % chunk == 0:
            samples.append(running)
    if len(samples) < 3:
        return {"divergence": False, "slope_pct": 0.0}
    last_seg = samples[-1] - samples[-2]
    prev_seg = samples[-2] - samples[-3]
    slope_total = samples[-1] - samples[0]
    # divergence: total CVD lower while price likely higher (the consumer checks)
    return {
        "divergence": last_seg < 0 and prev_seg < 0 and slope_total < 0,
        "slope_pct": slope_total / max(1.0, abs(samples[0]) + 1.0),
    }


def aggressive_sell_burst(state: SymbolStateData, window_sec: int = 60) -> bool:
    """Did sells dominate the most recent window with material notional?"""
    d = delta_window(state, window_sec=window_sec)
    return d["total"] > 0 and d["ratio"] < -0.35


def aggressive_buy_burst(state: SymbolStateData, window_sec: int = 60) -> bool:
    d = delta_window(state, window_sec=window_sec)
    return d["total"] > 0 and d["ratio"] > 0.35


def whale_print(state: SymbolStateData, *, min_notional: float = 250_000.0,
                window_sec: int = 120) -> bool:
    """Detect any single trade above the threshold inside the window."""
    if not state.trades:
        return False
    cutoff = state.trades[-1].ts_ms - window_sec * 1000
    for t in state.trades:
        if t.ts_ms < cutoff:
            continue
        if t.qty * t.price >= min_notional:
            return True
    return False


def buyer_exhaustion(state: SymbolStateData) -> bool:
    """Recent window shows high notional with shrinking buy share.

    Compares the 60s delta ratio to the prior 60-180s window. A meaningful
    drop indicates buyer interest is fading.
    """
    if len(state.trades) < 80:
        return False
    recent = delta_window(state, window_sec=60)
    prior = delta_window(state, window_sec=180)
    if recent["total"] < 5_000 or prior["total"] < 20_000:
        return False
    return (prior["ratio"] - recent["ratio"]) > 0.2
