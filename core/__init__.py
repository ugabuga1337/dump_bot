"""Core package.

Engine and Watchdog are NOT re-exported here on purpose: they import the
``signals`` and ``strategy`` packages, which themselves import from
``core.models`` / ``core.state``. Importing them at package init time would
create a circular import. Use ``from core.engine import Engine`` directly.
"""

from .market_regime import MarketRegimeDetector
from .models import (
    ConfidenceLabel,
    Kline,
    MarketRegime,
    MarkPriceTick,
    PumpDetection,
    SignalDecision,
    SymbolState,
    TradePrint,
)
from .state import SymbolStateData
from .universe import Universe

__all__ = [
    "ConfidenceLabel",
    "Kline",
    "MarkPriceTick",
    "MarketRegime",
    "MarketRegimeDetector",
    "PumpDetection",
    "SignalDecision",
    "SymbolState",
    "SymbolStateData",
    "TradePrint",
    "Universe",
]
