"""Shared dataclasses (engine-internal models).

All dataclasses use ``slots=True`` to keep per-symbol memory low.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SymbolState(str, Enum):
    """State machine for each symbol."""

    IDLE = "IDLE"
    WATCH = "WATCH"        # pump detected, looking for exhaustion
    ARMED = "ARMED"        # signal fired, in cooldown / outcome tracking
    COOLDOWN = "COOLDOWN"  # post-signal cooldown


class MarketRegime(str, Enum):
    BULL = "BULL"            # BTC trending up
    BEAR = "BEAR"            # BTC trending down
    CHOP = "CHOP"            # no trend
    HIGH_VOL = "HIGH_VOL"    # volatility shock
    NEUTRAL = "NEUTRAL"


class ConfidenceLabel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass(slots=True)
class Kline:
    """A single closed/closing kline (1m granularity).

    Kept tiny — only the fields the strategy actually uses.
    """

    open_ms: int
    close_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float           # base asset volume
    quote_volume: float     # quote asset notional
    trades: int
    taker_buy_vol: float    # base asset taker-buy volume (proxy for buy pressure)
    closed: bool = False    # True if the kline is final


@dataclass(slots=True)
class TradePrint:
    """Compact aggTrade summary kept in a rolling deque."""

    ts_ms: int
    price: float
    qty: float
    is_buyer_maker: bool    # if True, buyer was passive (taker is the seller)


@dataclass(slots=True)
class MarkPriceTick:
    ts_ms: int
    mark_price: float
    index_price: float
    funding_rate: float
    next_funding_ms: int


@dataclass(slots=True)
class PumpDetection:
    symbol: str
    ts_ms: int
    price: float
    pump_5m_pct: float
    pump_15m_pct: float
    volume_ratio: float
    oi_change_pct: float
    funding_rate: float
    relative_btc: float
    pump_score: float
    reasons: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalDecision:
    """Final assembled SHORT setup signal."""

    symbol: str
    ts_ms: int
    price: float
    pump_score: float
    exhaustion_score: float
    fake_pump_score: float
    confidence_score: float
    confidence_label: ConfidenceLabel
    market_regime: MarketRegime
    pump_5m_pct: float
    pump_15m_pct: float
    volume_ratio: float
    oi_change_pct: float
    funding_rate: float
    suggested_entry: float
    suggested_sl: float
    suggested_tp1: float
    suggested_tp2: float
    reasons: list[str] = field(default_factory=list)
    setup_tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
