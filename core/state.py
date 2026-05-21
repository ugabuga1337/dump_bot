"""Per-symbol state container.

Holds the rolling features used by all detectors. This is the working memory
of the system — it must stay small. Per-symbol footprint is roughly:

- 90 × 1m klines × ~80 bytes        ≈ 7.2 KB
- 600 aggTrade summaries × ~48 B    ≈ 28.8 KB
- a few EMAs / scalars              ≈ <0.5 KB
- last mark price + funding         ≈ <0.5 KB

For 120 symbols total RAM ≈ 4.5 MB just for working data — leaves plenty of
headroom on a 1GB VPS.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from utils import EMAState, RollingWindow

from .models import Kline, MarkPriceTick, SymbolState, TradePrint


@dataclass(slots=True)
class SymbolStateData:
    symbol: str

    state: SymbolState = SymbolState.IDLE
    state_since_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    last_signal_ms: int = 0
    last_pump_ms: int = 0

    # --- price/volume history (1m klines) ---
    klines: deque[Kline] = field(default_factory=lambda: deque(maxlen=90))   # 90m
    # Recent agg trades (rolling); enough to compute 5m delta/CVD.
    trades: deque[TradePrint] = field(default_factory=lambda: deque(maxlen=600))

    # --- rolling stats ---
    returns_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))
    volumes_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))
    quote_volumes_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))
    closes_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))
    highs_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))
    lows_1m: RollingWindow = field(default_factory=lambda: RollingWindow(maxlen=60))

    # --- streaming indicators ---
    ema_fast: EMAState = field(default_factory=lambda: EMAState(period=9))
    ema_slow: EMAState = field(default_factory=lambda: EMAState(period=21))

    # --- orderflow ---
    cvd: float = 0.0                  # cumulative volume delta over the trades deque
    last_delta_5m: float = 0.0
    last_delta_1m: float = 0.0

    # --- open interest ---
    oi_history: deque[tuple[int, float]] = field(default_factory=lambda: deque(maxlen=60))  # (ts_ms, oi)

    # --- mark/funding ---
    last_mark: MarkPriceTick | None = None

    # --- ticker stats from REST refresh ---
    quote_volume_24h: float = 0.0

    # --- meta ---
    last_update_ms: int = 0

    # ---------- helpers ----------

    def push_kline(self, k: Kline) -> None:
        if self.klines and self.klines[-1].open_ms == k.open_ms:
            # update in place: streaming kline updates from kline_1m channel
            self.klines[-1] = k
            # rolling stats remain based on closed klines; updated below if closed
        else:
            self.klines.append(k)
        if k.closed:
            # only feed closed klines into the streaming stats so we don't double count
            if self.closes_1m.last:
                prev = self.closes_1m.last
                ret = (k.close - prev) / prev * 100.0 if prev else 0.0
                self.returns_1m.push(ret)
            self.closes_1m.push(k.close)
            self.highs_1m.push(k.high)
            self.lows_1m.push(k.low)
            self.volumes_1m.push(k.volume)
            self.quote_volumes_1m.push(k.quote_volume)
            self.ema_fast.update(k.close)
            self.ema_slow.update(k.close)
        self.last_update_ms = k.close_ms

    def push_trade(self, t: TradePrint) -> None:
        # Track CVD over the trade buffer window.
        # If trade goes out of the deque, undo its delta first.
        if len(self.trades) == self.trades.maxlen:
            old = self.trades[0]
            sign = -1.0 if old.is_buyer_maker else 1.0
            self.cvd -= sign * old.qty * old.price
        sign = -1.0 if t.is_buyer_maker else 1.0
        self.cvd += sign * t.qty * t.price
        self.trades.append(t)

    def push_mark(self, m: MarkPriceTick) -> None:
        self.last_mark = m

    def push_oi(self, ts_ms: int, oi: float) -> None:
        self.oi_history.append((ts_ms, oi))

    # ---------- derived ----------

    def last_price(self) -> float:
        if self.klines:
            return self.klines[-1].close
        if self.last_mark is not None:
            return self.last_mark.mark_price
        return 0.0

    def oi_change_pct(self, lookback_min: int) -> float:
        if not self.oi_history:
            return 0.0
        now_ts, last = self.oi_history[-1]
        cutoff = now_ts - lookback_min * 60_000
        ref: float | None = None
        for ts, val in self.oi_history:
            if ts >= cutoff:
                ref = val
                break
        if ref is None or ref == 0:
            return 0.0
        return (last - ref) / ref * 100.0

    def recent_return_pct(self, minutes: int) -> float:
        """Return over the last N closed minutes (relative to first available point)."""
        if not self.klines:
            return 0.0
        if len(self.klines) < minutes + 1:
            ref = self.klines[0]
        else:
            ref = self.klines[-(minutes + 1)]
        last = self.klines[-1]
        if ref.close == 0:
            return 0.0
        return (last.close - ref.close) / ref.close * 100.0

    def recent_volume_ratio(self, minutes: int) -> float:
        """Sum of last ``minutes`` 1m volumes vs the mean of the prior baseline window."""
        if len(self.volumes_1m) < minutes + 3:
            return 0.0
        vols = self.volumes_1m.to_list()
        recent = sum(vols[-minutes:])
        baseline = vols[: max(1, len(vols) - minutes)]
        if not baseline:
            return 0.0
        mean = sum(baseline) / len(baseline)
        if mean == 0:
            return 0.0
        return (recent / minutes) / mean

    def avg_quote_volume(self, minutes: int = 30) -> float:
        if not self.quote_volumes_1m:
            return 0.0
        vols = self.quote_volumes_1m.to_list()[-minutes:]
        if not vols:
            return 0.0
        return sum(vols) / len(vols)

    def reset_after_signal(self, now_ms: int) -> None:
        """Move to COOLDOWN, drop transient state."""
        self.state = SymbolState.COOLDOWN
        self.state_since_ms = now_ms
        self.last_signal_ms = now_ms
