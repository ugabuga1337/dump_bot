"""Multi-factor pump detection.

This is the *gating* stage. We only move a symbol into WATCH mode once
several independent factors agree that a pump is happening. The output is
a (boolean, score, reasons) tuple — exhaustion analysis happens later.
"""

from __future__ import annotations

import logging

from config import PumpConfig
from core.market_regime import MarketRegimeDetector
from core.models import PumpDetection
from core.state import SymbolStateData
from utils import clamp, linear_score, now_ms, weighted_average

from . import features

log = logging.getLogger("pump")


DEFAULT_WEIGHTS: dict[str, float] = {
    "momentum": 0.30,
    "acceleration": 0.15,
    "volume": 0.20,
    "oi": 0.15,
    "funding": 0.05,
    "relative_btc": 0.10,
    "candle_structure": 0.05,
}


class PumpDetector:
    def __init__(
        self,
        cfg: PumpConfig,
        regime: MarketRegimeDetector,
        weights: dict[str, float] | None = None,
    ) -> None:
        self._cfg = cfg
        self._regime = regime
        self._weights = {**DEFAULT_WEIGHTS, **(weights or {})}

    def evaluate(self, state: SymbolStateData) -> PumpDetection | None:
        if len(state.klines) < 20 or len(state.closes_1m) < 20:
            return None

        pump_5m = state.recent_return_pct(5)
        pump_15m = state.recent_return_pct(15)

        # Cheap gate first — must show some directional move at all.
        if pump_5m < max(2.0, self._cfg.min_5m_pct * 0.5) and pump_15m < max(3.0, self._cfg.min_15m_pct * 0.5):
            return None

        vol_ratio = state.recent_volume_ratio(5)
        oi_change_5m = state.oi_change_pct(5)
        oi_change_15m = state.oi_change_pct(15)
        funding = state.last_mark.funding_rate * 100.0 if state.last_mark else 0.0  # %

        # Relative strength vs BTC trend over a comparable window
        btc_trend = self._regime.btc_trend_pct / max(1.0, self._regime._cfg.btc_trend_lookback) * 15.0  # noqa: SLF001
        rel_btc = pump_15m - btc_trend  # outperformance pct points

        # Momentum score: linear blend of 5m + 15m moves.
        s_momentum_5m = linear_score(pump_5m, self._cfg.min_5m_pct * 0.6, self._cfg.min_5m_pct * 1.8)
        s_momentum_15m = linear_score(pump_15m, self._cfg.min_15m_pct * 0.6, self._cfg.min_15m_pct * 1.8)
        s_momentum = (s_momentum_5m + s_momentum_15m) / 2.0

        accel = features.acceleration(state)
        s_accel = linear_score(accel, 0.5, 3.0)

        s_volume = linear_score(vol_ratio, self._cfg.min_volume_ratio * 0.6,
                                self._cfg.min_volume_ratio * 2.5)
        s_oi = linear_score(max(oi_change_5m, oi_change_15m / 1.5),
                            self._cfg.min_oi_pct * 0.5, self._cfg.min_oi_pct * 3.0)
        s_funding = linear_score(funding, 0.005, 0.06)  # 0.5 bp -> 6 bp
        s_rel_btc = linear_score(rel_btc, self._cfg.relative_btc * 0.5,
                                 self._cfg.relative_btc * 2.0)

        # Candle structure: are we seeing impulsive green sequences?
        impulsive = self._impulse_score(state)

        weighted_pieces = [
            (s_momentum, self._weights["momentum"]),
            (s_accel, self._weights["acceleration"]),
            (s_volume, self._weights["volume"]),
            (s_oi, self._weights["oi"]),
            (s_funding, self._weights["funding"]),
            (s_rel_btc, self._weights["relative_btc"]),
            (impulsive, self._weights["candle_structure"]),
        ]
        score = weighted_average(weighted_pieces) * 100.0
        score = clamp(score, 0.0, 100.0)

        if score < self._cfg.score_threshold:
            return None

        # Build reasons (only the ones that materially contribute).
        reasons: list[str] = []
        if pump_5m >= self._cfg.min_5m_pct:
            reasons.append(f"5m move +{pump_5m:.1f}%")
        if pump_15m >= self._cfg.min_15m_pct:
            reasons.append(f"15m move +{pump_15m:.1f}%")
        if accel > 0.5:
            reasons.append(f"price acceleration {accel:.2f}")
        if vol_ratio >= self._cfg.min_volume_ratio:
            reasons.append(f"volume spike {vol_ratio:.1f}x")
        if max(oi_change_5m, oi_change_15m) >= self._cfg.min_oi_pct:
            reasons.append(f"OI +{max(oi_change_5m, oi_change_15m):.1f}%")
        if funding > 0.03:
            reasons.append(f"funding overheated {funding:.3f}%")
        if rel_btc > self._cfg.relative_btc:
            reasons.append(f"outperforms BTC by {rel_btc:.1f}%")

        return PumpDetection(
            symbol=state.symbol,
            ts_ms=now_ms(),
            price=state.last_price(),
            pump_5m_pct=pump_5m,
            pump_15m_pct=pump_15m,
            volume_ratio=vol_ratio,
            oi_change_pct=max(oi_change_5m, oi_change_15m),
            funding_rate=funding,
            relative_btc=rel_btc,
            pump_score=score,
            reasons=reasons,
            metadata={
                "components": {
                    "momentum": s_momentum,
                    "acceleration": s_accel,
                    "volume": s_volume,
                    "oi": s_oi,
                    "funding": s_funding,
                    "relative_btc": s_rel_btc,
                    "candle_structure": impulsive,
                }
            },
        )

    @staticmethod
    def _impulse_score(state: SymbolStateData) -> float:
        """Fraction of last 8 candles that closed green AND with strong body."""
        klines = list(state.klines)[-8:]
        if not klines:
            return 0.0
        greens = 0
        for k in klines:
            if k.close > k.open:
                rng = max(k.high - k.low, 1e-9)
                body = (k.close - k.open) / rng
                if body > 0.55:
                    greens += 1
        return greens / len(klines)
