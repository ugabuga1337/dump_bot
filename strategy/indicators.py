"""Secondary technical indicators.

Per the spec these are confirmations only — not primary signal sources.
Implementations stay stream-friendly (no DataFrame churn).
"""

from __future__ import annotations

from core.state import SymbolStateData


def rsi(state: SymbolStateData, period: int = 14) -> float:
    """Wilder RSI on closing prices of the streamed klines."""
    closes = state.closes_1m.to_list()
    if len(closes) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    # Seed
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    # Wilder smoothing for the remainder
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_bearish_divergence(state: SymbolStateData, lookback: int = 20) -> bool:
    """Classic divergence: price makes a higher high while RSI makes a lower high.

    Cheap version: compare last two local maxima on a small window.
    """
    closes = state.closes_1m.to_list()
    if len(closes) < lookback + 16:
        return False
    rsi_values = _rolling_rsi(closes, period=14)
    if len(rsi_values) < lookback + 2:
        return False
    # Take last "lookback" points and find the two highest peaks among them.
    recent_closes = closes[-lookback:]
    recent_rsi = rsi_values[-lookback:]
    # Identify two highest highs.
    idx_sorted = sorted(range(lookback), key=lambda i: recent_closes[i], reverse=True)
    top1, top2 = idx_sorted[0], idx_sorted[1]
    if top1 < top2:
        top1, top2 = top2, top1  # ensure top1 is the later one
    if abs(top1 - top2) < 3:
        return False
    price_hh = recent_closes[top1] > recent_closes[top2]
    rsi_lh = recent_rsi[top1] < recent_rsi[top2]
    return price_hh and rsi_lh


def _rolling_rsi(closes: list[float], period: int = 14) -> list[float]:
    out: list[float] = []
    if len(closes) < period + 1:
        return out
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / avg_loss if avg_loss else float("inf")
    out.append(100.0 - 100.0 / (1.0 + rs) if avg_loss else 100.0)
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss else float("inf")
        out.append(100.0 - 100.0 / (1.0 + rs) if avg_loss else 100.0)
    return out


def session_vwap(state: SymbolStateData, lookback: int = 60) -> float:
    """Anchored VWAP over the last ``lookback`` closed minutes."""
    klines = list(state.klines)[-lookback:]
    if not klines:
        return 0.0
    num = 0.0
    den = 0.0
    for k in klines:
        typical = (k.high + k.low + k.close) / 3.0
        num += typical * k.volume
        den += k.volume
    return num / den if den else 0.0


def vwap_deviation_pct(state: SymbolStateData, lookback: int = 60) -> float:
    vw = session_vwap(state, lookback=lookback)
    last = state.last_price()
    if vw == 0:
        return 0.0
    return (last - vw) / vw * 100.0
