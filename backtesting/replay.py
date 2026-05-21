"""Lightweight backtesting engine.

Replays historical 1m klines through the pump + exhaustion pipeline and
reports aggregate signal statistics. We can't reconstruct genuine CVD /
agg-trade flow from klines alone — instead the backtester runs with a
modified scorer that drops orderflow components and reweights the rest.

This is a research tool, not a 1:1 simulation of the live engine. It is
designed to be cheap enough to run on a 1GB VPS overnight against months of
data for parameter scans.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

from config import get_settings
from connectors import BinanceFuturesREST
from core.market_regime import MarketRegimeDetector
from core.models import Kline
from core.state import SymbolStateData
from signals import ConfidenceScorer
from strategy import ExhaustionScorer, FakePumpDetector, PumpDetector

log = logging.getLogger("backtest")


@dataclass(slots=True)
class BacktestResult:
    symbol: str
    total: int
    avg_fav_pct: float
    avg_adv_pct: float
    wins: int
    losses: int


class Backtester:
    def __init__(self, settings=None) -> None:
        self.settings = settings or get_settings()
        self.rest = BinanceFuturesREST(self.settings.binance.rest_url)
        self.regime = MarketRegimeDetector(self.settings.regime, self.rest)
        self.pump = PumpDetector(self.settings.pump, self.regime)
        self.exhaustion = ExhaustionScorer(min_confirmations=2)  # relax for klines-only
        self.fake_pump = FakePumpDetector()
        self.scorer = ConfidenceScorer(self.settings.signal, self.regime)

    async def fetch_klines(self, symbol: str, *, days: int = 7) -> list[Kline]:
        """Pull up to ``days`` of 1m klines (Binance returns max 1500 per call)."""
        klines: list[Kline] = []
        end = None
        rounds = max(1, (days * 1440 // 1500) + 1)
        for _ in range(rounds):
            batch = await self.rest.klines(symbol, interval="1m", limit=1500, end_time=end)
            if not batch:
                break
            for row in batch:
                klines.append(Kline(
                    open_ms=int(row[0]), close_ms=int(row[6]),
                    open=float(row[1]), high=float(row[2]),
                    low=float(row[3]), close=float(row[4]),
                    volume=float(row[5]), quote_volume=float(row[7]),
                    trades=int(row[8]), taker_buy_vol=float(row[9]),
                    closed=True,
                ))
            end = int(batch[0][0]) - 1
            if len(batch) < 1500:
                break
        klines.sort(key=lambda k: k.open_ms)
        return klines

    async def run_symbol(self, symbol: str, *, days: int = 7,
                         track_minutes: int = 60) -> BacktestResult:
        klines = await self.fetch_klines(symbol, days=days)
        log.info("backtest_loaded",
                 extra={"symbol": symbol, "candles": len(klines)})

        state = SymbolStateData(symbol=symbol)
        results: list[tuple[int, float, float]] = []  # (created_idx, fav, adv)
        cooldown_until = -1

        for i, k in enumerate(klines):
            state.push_kline(k)
            if i < cooldown_until:
                continue
            pump = self.pump.evaluate(state)
            if not pump:
                continue
            exh = self.exhaustion.evaluate(state)
            fake = self.fake_pump.evaluate(state)
            scored = self.scorer.evaluate(state, pump, exh, fake)
            if not scored.fire or scored.decision is None:
                continue
            # Track favorable/adverse over the next `track_minutes`
            entry = scored.decision.suggested_entry
            tracking_end = min(len(klines) - 1, i + track_minutes)
            max_fav = 0.0
            max_adv = 0.0
            for j in range(i, tracking_end + 1):
                p = klines[j].low
                if entry:
                    fav = (entry - p) / entry * 100.0
                    if fav > max_fav:
                        max_fav = fav
                p = klines[j].high
                if entry:
                    adv = (p - entry) / entry * 100.0
                    if adv > max_adv:
                        max_adv = adv
            results.append((i, max_fav, max_adv))
            cooldown_until = i + int(self.settings.signal.cooldown_sec / 60)

        if not results:
            return BacktestResult(symbol=symbol, total=0, avg_fav_pct=0,
                                  avg_adv_pct=0, wins=0, losses=0)

        avg_fav = sum(r[1] for r in results) / len(results)
        avg_adv = sum(r[2] for r in results) / len(results)
        wins = sum(1 for r in results if r[1] >= 1.5 and r[2] < 2.0)
        losses = len(results) - wins
        return BacktestResult(symbol=symbol, total=len(results), avg_fav_pct=avg_fav,
                              avg_adv_pct=avg_adv, wins=wins, losses=losses)

    async def run(self, symbols: list[str], *, days: int = 7) -> list[BacktestResult]:
        async with self.rest:
            await self.regime.refresh()
            results: list[BacktestResult] = []
            for s in symbols:
                try:
                    res = await self.run_symbol(s, days=days)
                    results.append(res)
                    log.info("backtest_symbol_done", extra={
                        "symbol": s,
                        "total": res.total,
                        "avg_fav_pct": round(res.avg_fav_pct, 2),
                        "avg_adv_pct": round(res.avg_adv_pct, 2),
                        "wins": res.wins,
                    })
                except Exception as exc:  # noqa: BLE001
                    log.warning("backtest_symbol_failed",
                                extra={"symbol": s, "err": repr(exc)})
            return results


async def amain(symbols: list[str], days: int) -> None:
    bt = Backtester()
    results = await bt.run(symbols, days=days)
    print(json.dumps([r.__dict__ for r in results], indent=2, default=str))


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Dump Bot backtester")
    parser.add_argument("symbols", nargs="+", help="symbols to backtest, e.g. DOGEUSDT")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    asyncio.run(amain([s.upper() for s in args.symbols], args.days))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
