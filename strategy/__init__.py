"""Strategy package — pure stateless analytics over SymbolStateData."""

from .exhaustion import ExhaustionResult, ExhaustionScorer
from .fake_pump import FakePumpDetector, FakePumpResult
from .indicators import (
    rsi,
    rsi_bearish_divergence,
    session_vwap,
    vwap_deviation_pct,
)
from .microstructure import (
    blowoff_top,
    buyer_exhaustion,
    climax_volume,
    cvd_divergence,
    delta_window,
    liquidity_grab,
    momentum_slowdown,
    oi_flat_after_pump,
    whale_print,
    wick_dominated,
)
from .pump_detector import PumpDetector

__all__ = [
    "ExhaustionResult",
    "ExhaustionScorer",
    "FakePumpDetector",
    "FakePumpResult",
    "PumpDetector",
    "blowoff_top",
    "buyer_exhaustion",
    "climax_volume",
    "cvd_divergence",
    "delta_window",
    "liquidity_grab",
    "momentum_slowdown",
    "oi_flat_after_pump",
    "rsi",
    "rsi_bearish_divergence",
    "session_vwap",
    "vwap_deviation_pct",
    "whale_print",
    "wick_dominated",
]
