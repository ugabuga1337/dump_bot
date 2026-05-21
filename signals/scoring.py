"""Confidence scoring and signal assembly.

Confidence blends:
- Exhaustion score (primary; the actual reversal evidence)
- Pump score (we want to short genuinely overheated moves only)
- Fake pump score (penalty: deduct from confidence)
- Regime multiplier (BTC bear / chop -> easier shorts)
- Optional whale bonus

The output also includes suggested entry / SL / TP zones built from recent
ATR, which the user can copy directly into a manual short setup.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config import SignalConfig
from core.market_regime import MarketRegimeDetector
from core.models import (
    ConfidenceLabel,
    MarketRegime,
    PumpDetection,
    SignalDecision,
)
from core.state import SymbolStateData
from strategy.exhaustion import ExhaustionResult
from strategy.fake_pump import FakePumpResult
from strategy.features import atr_pct
from strategy.microstructure import whale_print
from utils import clamp, now_ms

DEFAULT_WEIGHTS: dict[str, float] = {
    "exhaustion": 0.55,
    "pump": 0.20,
    "fake_pump_penalty": 0.10,
    "regime": 0.10,
    "whale": 0.05,
}


@dataclass(slots=True)
class ScoredDecision:
    fire: bool
    confidence: float
    label: ConfidenceLabel
    decision: SignalDecision | None
    debug: dict[str, Any]


class ConfidenceScorer:
    def __init__(
        self,
        signal_cfg: SignalConfig,
        regime: MarketRegimeDetector,
        weights: dict[str, float] | None = None,
    ) -> None:
        self._cfg = signal_cfg
        self._regime = regime
        self._weights = {**DEFAULT_WEIGHTS, **(weights or {})}

    def evaluate(
        self,
        state: SymbolStateData,
        pump: PumpDetection,
        exhaustion: ExhaustionResult,
        fake: FakePumpResult,
    ) -> ScoredDecision:
        # Base building blocks (0..1)
        exh = exhaustion.score / 100.0
        pmp = pump.pump_score / 100.0
        fkp = fake.score / 100.0
        reg = self._regime.short_setup_friendly()  # already 0..1
        whale = 1.0 if whale_print(state) else 0.0

        weighted = (
            exh * self._weights["exhaustion"]
            + pmp * self._weights["pump"]
            + reg * self._weights["regime"]
            + whale * self._weights["whale"]
        )
        # Penalty term: fake pump score is multiplied negatively
        penalty = fkp * self._weights["fake_pump_penalty"]
        raw = (weighted - penalty)
        # Normalize by gross weight (excluding the penalty so the scale is intuitive)
        gross = sum(v for k, v in self._weights.items() if k != "fake_pump_penalty")
        norm = clamp(raw / max(gross, 1e-9), 0.0, 1.0)
        confidence = clamp(norm * 100.0, 0.0, 100.0)

        # Threshold gate
        fires = (
            exhaustion.score >= self._cfg.exhaustion_threshold
            and confidence >= self._cfg.confidence_low
        )

        if confidence >= self._cfg.confidence_high:
            label = ConfidenceLabel.HIGH
        elif confidence >= self._cfg.confidence_med:
            label = ConfidenceLabel.MEDIUM
        else:
            label = ConfidenceLabel.LOW

        decision: SignalDecision | None = None
        if fires:
            decision = self._build_decision(state, pump, exhaustion, fake, confidence, label)

        debug = {
            "exhaustion": round(exhaustion.score, 2),
            "pump": round(pump.pump_score, 2),
            "fake": round(fake.score, 2),
            "regime_multiplier": round(reg, 2),
            "regime": self._regime.regime.value,
            "whale": whale,
            "confidence": round(confidence, 2),
            "fire_threshold": {
                "exhaustion_min": self._cfg.exhaustion_threshold,
                "confidence_min": self._cfg.confidence_low,
            },
        }

        return ScoredDecision(
            fire=fires, confidence=confidence, label=label,
            decision=decision, debug=debug,
        )

    def _build_decision(
        self,
        state: SymbolStateData,
        pump: PumpDetection,
        exhaustion: ExhaustionResult,
        fake: FakePumpResult,
        confidence: float,
        label: ConfidenceLabel,
    ) -> SignalDecision:
        price = state.last_price() or pump.price
        atr = atr_pct(state, period=14)
        atr_value = (atr / 100.0) * price if atr else 0.0
        # SL above the recent swing high; TP1/TP2 below entry by ATR multiples.
        recent_highs = [k.high for k in list(state.klines)[-20:]] if state.klines else [price]
        swing_high = max(recent_highs) if recent_highs else price
        entry = price * 0.999  # short entry near current price
        sl = max(swing_high * 1.005, entry * (1 + max(atr / 100.0 * 1.2, 0.012)))
        tp1 = entry * (1 - max(atr / 100.0 * 1.5, 0.018))
        tp2 = entry * (1 - max(atr / 100.0 * 2.5, 0.030))

        market_regime = self._regime.regime if isinstance(self._regime.regime, MarketRegime) else MarketRegime.NEUTRAL

        reasons: list[str] = list(exhaustion.reasons)
        if fake.flags:
            # We don't add these as positive reasons, but include in metadata.
            pass

        setup_tags: list[str] = list(exhaustion.tags)
        # Add a quick aggregate tag for analytics if multiple high-confidence components fire.
        if len(setup_tags) >= 4:
            setup_tags.append("multi_confirmation")

        return SignalDecision(
            symbol=state.symbol,
            ts_ms=now_ms(),
            price=price,
            pump_score=pump.pump_score,
            exhaustion_score=exhaustion.score,
            fake_pump_score=fake.score,
            confidence_score=confidence,
            confidence_label=label,
            market_regime=market_regime,
            pump_5m_pct=pump.pump_5m_pct,
            pump_15m_pct=pump.pump_15m_pct,
            volume_ratio=pump.volume_ratio,
            oi_change_pct=pump.oi_change_pct,
            funding_rate=pump.funding_rate,
            suggested_entry=round(entry, 8),
            suggested_sl=round(sl, 8),
            suggested_tp1=round(tp1, 8),
            suggested_tp2=round(tp2, 8),
            reasons=reasons,
            setup_tags=setup_tags,
            metadata={
                "atr_pct": atr,
                "atr_value": atr_value,
                "fake_pump_flags": fake.flags,
                "exhaustion_debug": exhaustion.debug,
                "pump_components": pump.metadata.get("components", {}),
                "btc_trend_pct": self._regime.btc_trend_pct,
            },
        )
