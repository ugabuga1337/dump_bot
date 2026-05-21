"""BTC market regime detector.

Computes a coarse market regime from BTC 1h history. Cheap to refresh,
called periodically by the engine.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from config import RegimeConfig
from connectors import BinanceFuturesREST

from .models import MarketRegime

log = logging.getLogger("regime")


class MarketRegimeDetector:
    def __init__(self, cfg: RegimeConfig, rest: BinanceFuturesREST) -> None:
        self._cfg = cfg
        self._rest = rest
        self.regime: MarketRegime = MarketRegime.NEUTRAL
        self.btc_price: float = 0.0
        self.btc_trend_pct: float = 0.0
        self.btc_atr_pct: float = 0.0
        self.last_refresh_ms: int = 0

    async def refresh(self) -> dict[str, Any]:
        try:
            klines = await self._rest.klines(
                self._cfg.btc_symbol, interval="1h", limit=self._cfg.btc_trend_lookback
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("regime_refresh_failed", extra={"err": repr(exc)})
            return self.as_dict()

        if not klines or len(klines) < 30:
            return self.as_dict()

        closes = [float(k[4]) for k in klines]
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]

        self.btc_price = closes[-1]
        # Slope over the lookback (linear regression OLS, hand-rolled, O(n))
        n = len(closes)
        xs = list(range(n))
        mean_x = sum(xs) / n
        mean_y = sum(closes) / n
        num = sum(
            (x - mean_x) * (y - mean_y)
            for x, y in zip(xs, closes, strict=False)
        )
        den = sum((x - mean_x) ** 2 for x in xs)
        slope = num / den if den else 0.0
        # Convert slope into percentage move over the full window.
        first = closes[0] or 1.0
        trend_pct = (slope * (n - 1)) / first * 100.0
        self.btc_trend_pct = trend_pct

        # ATR% over the configured period.
        period = self._cfg.btc_atr_period
        atr_period = period if len(closes) >= period + 1 else len(closes) - 1
        tr_sum = 0.0
        for i in range(len(closes) - atr_period, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1] if i > 0 else closes[i]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_sum += tr
        atr = tr_sum / atr_period if atr_period else 0.0
        self.btc_atr_pct = (atr / self.btc_price) * 100.0 if self.btc_price else 0.0

        # Decide regime
        if self.btc_atr_pct > 3.0:
            self.regime = MarketRegime.HIGH_VOL
        elif trend_pct > 4.0:
            self.regime = MarketRegime.BULL
        elif trend_pct < -4.0:
            self.regime = MarketRegime.BEAR
        elif math.fabs(trend_pct) < 1.5:
            self.regime = MarketRegime.CHOP
        else:
            self.regime = MarketRegime.NEUTRAL

        log.info("regime", extra={
            "regime": self.regime.value,
            "btc_price": round(self.btc_price, 2),
            "trend_pct": round(self.btc_trend_pct, 2),
            "atr_pct": round(self.btc_atr_pct, 3),
        })
        return self.as_dict()

    def as_dict(self) -> dict[str, Any]:
        return {
            "regime": self.regime.value,
            "btc_price": self.btc_price,
            "btc_trend_pct": self.btc_trend_pct,
            "btc_atr_pct": self.btc_atr_pct,
        }

    def short_setup_friendly(self) -> float:
        """Return a [0,1] multiplier that biases signals toward shorts when regime fits.

        Strong uptrend in BTC -> we're MORE conservative about shorting alts that pump.
        Bear / chop -> shorts are favored.
        """
        if self.regime == MarketRegime.BEAR:
            return 1.0
        if self.regime == MarketRegime.CHOP:
            return 0.85
        if self.regime == MarketRegime.HIGH_VOL:
            return 0.7
        if self.regime == MarketRegime.NEUTRAL:
            return 0.8
        # Bull => more risk of continuation
        return 0.55
