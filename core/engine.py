"""Engine orchestrator.

Owns:
- Universe lifecycle
- WebSocket multiplexer
- Per-symbol state cache
- Pump / exhaustion / fake-pump evaluation pipeline
- Telegram notifier + signal anti-spam
- Outcome tracker
- BTC market regime refresh
- Periodic OI polling (no public WS for OI yet)
- Heartbeat + watchdog
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

from config import Settings, get_settings
from connectors import BinanceFuturesREST, StreamManager
from core.market_regime import MarketRegimeDetector
from core.models import (
    Kline,
    MarkPriceTick,
    SymbolState,
    TradePrint,
)
from core.state import SymbolStateData
from core.universe import Universe
from signals import AntiSpam, ConfidenceScorer, OutcomeTracker
from storage import Database, Repository, open_database
from strategy import ExhaustionScorer, FakePumpDetector, PumpDetector
from telegram import TelegramNotifier, format_signal
from utils import now_ms, setup_logging

log = logging.getLogger("engine")


class Engine:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        weights = self.settings.raw_yaml or {}
        self.rest = BinanceFuturesREST(self.settings.binance.rest_url)
        self.regime = MarketRegimeDetector(self.settings.regime, self.rest)
        self.universe = Universe(self.settings.universe, self.rest)
        self.pump_detector = PumpDetector(
            self.settings.pump, self.regime, weights=weights.get("pump_weights"),
        )
        self.exhaustion = ExhaustionScorer(
            weights=weights.get("exhaustion_weights"),
            min_confirmations=int(weights.get("min_confirmations", 3)),
        )
        self.fake_pump = FakePumpDetector(weights=weights.get("fake_pump_weights"))
        self.scorer = ConfidenceScorer(
            self.settings.signal, self.regime,
            weights=weights.get("confidence_weights"),
        )
        self.anti_spam = AntiSpam(
            cooldown_sec=self.settings.signal.cooldown_sec,
            watch_ttl_sec=self.settings.signal.watch_ttl_sec,
        )
        self.telegram = TelegramNotifier(self.settings.telegram)
        self.db: Database | None = None
        self.repo: Repository | None = None
        self.outcome_tracker: OutcomeTracker | None = None

        self.stream_manager = StreamManager(
            self.settings.binance.ws_url, self._on_ws_message
        )

        self.symbols: list[str] = []
        self.state: dict[str, SymbolStateData] = {}

        self._heartbeat_path = self.settings.data_dir / "heartbeat"

        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self._messages_seen = 0
        self._signals_emitted = 0
        self._pumps_detected = 0
        self._started_at_ms = now_ms()

    # ------------- lifecycle -------------

    async def start(self) -> None:
        setup_logging(self.settings.log_level, json_output=self.settings.log_json)
        log.info("engine_starting")

        self.db = await open_database(self.settings.db_path)
        self.repo = Repository(self.db)
        self.outcome_tracker = OutcomeTracker(self.settings.outcome, self.repo)
        await self.outcome_tracker.restore_open()

        await self.rest.__aenter__()
        try:
            await self._initial_universe_refresh()
            await self.regime.refresh()
        except Exception as exc:  # noqa: BLE001
            log.error("engine_bootstrap_failed", extra={"err": repr(exc)}, exc_info=True)
            raise

        await self._warmup_klines()
        await self._start_streams()
        await self.telegram.start()

        self._tasks.extend([
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._universe_refresh_loop(), name="universe-refresh"),
            asyncio.create_task(self._regime_refresh_loop(), name="regime-refresh"),
            asyncio.create_task(self._oi_polling_loop(), name="oi-polling"),
            asyncio.create_task(self._market_snapshot_loop(), name="market-snapshot"),
        ])

        log.info("engine_started", extra={"symbols": len(self.symbols)})

    async def shutdown(self) -> None:
        log.info("engine_shutting_down")
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.stream_manager.stop()
        await self.telegram.stop()
        with contextlib.suppress(Exception):
            await self.rest.__aexit__(None, None, None)
        if self.db:
            await self.db.close()
        log.info("engine_stopped")

    # ------------- bootstrap -------------

    async def _initial_universe_refresh(self) -> None:
        symbols = await self.universe.refresh()
        if not symbols:
            raise RuntimeError("no symbols matched the universe filters")
        self.symbols = symbols
        for s in symbols:
            self.state.setdefault(s, SymbolStateData(symbol=s))
        # add regime symbol too so we can refresh its klines easily
        self.state.setdefault(self.settings.regime.btc_symbol,
                              SymbolStateData(symbol=self.settings.regime.btc_symbol))
        if self.repo:
            await self.repo.upsert_symbols(self.universe.export_rows())

    async def _warmup_klines(self) -> None:
        """Backfill 60m of 1m klines for each symbol.

        Sequential, but with bounded concurrency to keep RAM low on a 1GB VPS.
        """
        sem = asyncio.Semaphore(6)
        async def fetch(sym: str, _sem: asyncio.Semaphore = sem) -> None:
            async with _sem:
                try:
                    klines = await self.rest.klines(sym, interval="1m", limit=60)
                except Exception as exc:  # noqa: BLE001
                    log.warning("warmup_failed", extra={"sym": sym, "err": repr(exc)})
                    return
                st = self.state[sym]
                for k in klines:
                    st.push_kline(_kline_from_rest(k))

        await asyncio.gather(*(fetch(s) for s in self.symbols))
        log.info("warmup_complete")

    async def _start_streams(self) -> None:
        streams = self._build_stream_list(self.symbols)
        await self.stream_manager.start(streams)

    def _build_stream_list(self, symbols: list[str]) -> list[str]:
        streams: list[str] = []
        for s in symbols:
            sym = s.lower()
            streams.append(f"{sym}@aggTrade")
            streams.append(f"{sym}@kline_1m")
            streams.append(f"{sym}@markPrice@1s")
        return streams

    # ------------- background loops -------------

    async def _heartbeat_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                self._heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
                self._heartbeat_path.write_text(str(int(time.time())))
            except Exception as exc:  # noqa: BLE001
                log.debug("heartbeat_write_failed", extra={"err": repr(exc)})
            await asyncio.sleep(20)

    async def _universe_refresh_loop(self) -> None:
        while not self._stopping.is_set():
            await asyncio.sleep(self.settings.universe.refresh_minutes * 60)
            if self._stopping.is_set():
                return
            try:
                new_symbols = await self.universe.refresh()
                if not new_symbols:
                    continue
                added = set(new_symbols) - set(self.symbols)
                removed = set(self.symbols) - set(new_symbols)
                if added or removed:
                    log.info("universe_changed",
                             extra={"added": list(added), "removed": list(removed)})
                    self.symbols = new_symbols
                    for s in added:
                        self.state.setdefault(s, SymbolStateData(symbol=s))
                    for s in removed:
                        self.state.pop(s, None)
                    await self.stream_manager.update_streams(self._build_stream_list(new_symbols))
                if self.repo:
                    await self.repo.upsert_symbols(self.universe.export_rows())
            except Exception as exc:  # noqa: BLE001
                log.warning("universe_refresh_failed", extra={"err": repr(exc)})

    async def _regime_refresh_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.regime.refresh()
            except Exception as exc:  # noqa: BLE001
                log.warning("regime_refresh_failed", extra={"err": repr(exc)})
            await asyncio.sleep(300)

    async def _oi_polling_loop(self) -> None:
        """Open interest doesn't have a free public WS — poll REST every 60s."""
        while not self._stopping.is_set():
            try:
                snapshot = await self.rest.book_ticker()  # cheap to ride along
                _ = snapshot  # noop today; placeholder for spread analytics
            except Exception:
                pass

            sem = asyncio.Semaphore(8)
            async def poll(sym: str, _sem: asyncio.Semaphore = sem) -> None:
                async with _sem:
                    try:
                        data = await self.rest.open_interest(sym)
                        oi = float(data.get("openInterest", 0.0))
                        ts = int(data.get("time") or now_ms())
                        st = self.state.get(sym)
                        if st is not None:
                            st.push_oi(ts, oi)
                    except Exception:  # noqa: BLE001
                        pass

            await asyncio.gather(*(poll(s) for s in list(self.symbols)))
            await asyncio.sleep(60)

    async def _market_snapshot_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                if self.repo:
                    overheated = sum(
                        1 for st in self.state.values()
                        if st.recent_return_pct(15) >= self.settings.pump.min_15m_pct
                    )
                    vol_avg = 0.0
                    if self.state:
                        ratios = [st.recent_volume_ratio(5) for st in self.state.values()]
                        ratios = [r for r in ratios if r > 0]
                        if ratios:
                            vol_avg = sum(ratios) / len(ratios)
                    await self.repo.insert_market_snapshot({
                        "btc_price": self.regime.btc_price,
                        "btc_regime": self.regime.regime.value,
                        "btc_trend_pct": self.regime.btc_trend_pct,
                        "btc_atr_pct": self.regime.btc_atr_pct,
                        "avg_universe_vol": vol_avg,
                        "overheated_count": overheated,
                        "metadata": {
                            "messages_seen": self._messages_seen,
                            "signals_emitted": self._signals_emitted,
                            "pumps_detected": self._pumps_detected,
                            "ws_shards": self.stream_manager.stats(),
                            "telegram": self.telegram.stats,
                        },
                    })
            except Exception as exc:  # noqa: BLE001
                log.warning("snapshot_failed", extra={"err": repr(exc)})
            await asyncio.sleep(60)

    # ------------- WS message router -------------

    async def _on_ws_message(self, payload: dict[str, Any]) -> None:
        self._messages_seen += 1
        # combined stream payload: {"stream": "btcusdt@aggTrade", "data": {...}}
        stream = payload.get("stream", "")
        data = payload.get("data") or payload
        if not stream:
            return
        # We don't bother parsing the stream string — go by the event type
        ev = data.get("e")
        symbol = data.get("s") or stream.split("@", 1)[0].upper()

        st = self.state.get(symbol)
        if st is None:
            return

        try:
            if ev == "aggTrade":
                t = TradePrint(
                    ts_ms=int(data["T"]),
                    price=float(data["p"]),
                    qty=float(data["q"]),
                    is_buyer_maker=bool(data.get("m", False)),
                )
                st.push_trade(t)
                # feed outcome tracker on any tick (cheap)
                if self.outcome_tracker is not None and symbol in self.outcome_tracker.tracked_symbols():
                    await self.outcome_tracker.on_price(symbol, t.price, t.ts_ms)
            elif ev == "kline":
                k_raw = data["k"]
                k = Kline(
                    open_ms=int(k_raw["t"]),
                    close_ms=int(k_raw["T"]),
                    open=float(k_raw["o"]),
                    high=float(k_raw["h"]),
                    low=float(k_raw["l"]),
                    close=float(k_raw["c"]),
                    volume=float(k_raw["v"]),
                    quote_volume=float(k_raw["q"]),
                    trades=int(k_raw["n"]),
                    taker_buy_vol=float(k_raw["V"]),
                    closed=bool(k_raw["x"]),
                )
                st.push_kline(k)
                # Re-evaluate on candle close (cheap, deterministic)
                if k.closed:
                    await self._evaluate(symbol)
            elif ev == "markPriceUpdate":
                m = MarkPriceTick(
                    ts_ms=int(data["E"]),
                    mark_price=float(data["p"]),
                    index_price=float(data["i"]),
                    funding_rate=float(data["r"]),
                    next_funding_ms=int(data.get("T") or 0),
                )
                st.push_mark(m)
        except Exception as exc:  # noqa: BLE001
            log.debug("ws_payload_parse_failed", extra={"err": repr(exc), "ev": ev})

    # ------------- evaluation pipeline -------------

    async def _evaluate(self, symbol: str) -> None:
        st = self.state.get(symbol)
        if st is None:
            return

        # State transitions
        if st.state == SymbolState.COOLDOWN:
            if self.anti_spam.cooldown_remaining_sec(symbol) == 0:
                st.state = SymbolState.IDLE
                st.state_since_ms = now_ms()
        if st.state == SymbolState.WATCH and self.anti_spam.watch_expired(st.state_since_ms):
            st.state = SymbolState.IDLE
            st.state_since_ms = now_ms()
            log.debug("watch_expired", extra={"symbol": symbol})

        # Stage 1: pump gate
        if st.state == SymbolState.IDLE:
            pump = self.pump_detector.evaluate(st)
            if not pump:
                return
            self._pumps_detected += 1
            st.state = SymbolState.WATCH
            st.state_since_ms = now_ms()
            st.last_pump_ms = pump.ts_ms
            if self.repo:
                await self.repo.insert_pump({
                    "symbol": symbol,
                    "detected_ms": pump.ts_ms,
                    "price": pump.price,
                    "pump_5m_pct": pump.pump_5m_pct,
                    "pump_15m_pct": pump.pump_15m_pct,
                    "volume_ratio": pump.volume_ratio,
                    "oi_change_pct": pump.oi_change_pct,
                    "funding_rate": pump.funding_rate,
                    "pump_score": pump.pump_score,
                    "relative_btc": pump.relative_btc,
                    "reasons": pump.reasons,
                    "metadata": pump.metadata,
                })
            log.info("pump_detected",
                     extra={"symbol": symbol, "score": round(pump.pump_score, 1),
                            "5m": round(pump.pump_5m_pct, 2),
                            "15m": round(pump.pump_15m_pct, 2)})
            return

        # Stage 2: exhaustion analysis (only in WATCH)
        if st.state == SymbolState.WATCH:
            pump = self.pump_detector.evaluate(st)
            if not pump:
                return
            exhaustion = self.exhaustion.evaluate(st)
            fake = self.fake_pump.evaluate(st)
            scored = self.scorer.evaluate(st, pump, exhaustion, fake)

            if not scored.fire or scored.decision is None:
                return

            can, reason = self.anti_spam.can_fire(
                symbol, scored.confidence, scored.decision.setup_tags,
            )
            if not can:
                log.debug("signal_suppressed",
                          extra={"symbol": symbol, "reason": reason,
                                 "confidence": round(scored.confidence, 1)})
                return

            await self._emit_signal(st, scored)

    async def _emit_signal(self, st: SymbolStateData, scored) -> None:  # type: ignore[no-untyped-def]
        d = scored.decision
        if d is None or self.repo is None:
            return
        signal_id = await self.repo.insert_signal({
            "symbol": d.symbol,
            "created_ms": d.ts_ms,
            "price": d.price,
            "pump_5m_pct": d.pump_5m_pct,
            "pump_15m_pct": d.pump_15m_pct,
            "pump_score": d.pump_score,
            "exhaustion_score": d.exhaustion_score,
            "fake_pump_score": d.fake_pump_score,
            "confidence_score": d.confidence_score,
            "confidence_label": d.confidence_label.value,
            "market_regime": d.market_regime.value,
            "volume_ratio": d.volume_ratio,
            "oi_change_pct": d.oi_change_pct,
            "funding_rate": d.funding_rate,
            "suggested_entry": d.suggested_entry,
            "suggested_sl": d.suggested_sl,
            "suggested_tp1": d.suggested_tp1,
            "suggested_tp2": d.suggested_tp2,
            "reasons": d.reasons,
            "setup_tags": d.setup_tags,
            "metadata": d.metadata,
        })
        self.anti_spam.record_fire(d.symbol, d.confidence_score, d.setup_tags)
        st.reset_after_signal(d.ts_ms)
        self._signals_emitted += 1
        log.info("signal", extra={
            "symbol": d.symbol,
            "score": round(d.confidence_score, 1),
            "exhaustion": round(d.exhaustion_score, 1),
            "tags": ",".join(d.setup_tags),
        })
        try:
            await self.telegram.send(format_signal(d))
        except Exception as exc:  # noqa: BLE001
            log.warning("telegram_enqueue_failed", extra={"err": repr(exc)})

        if self.outcome_tracker is not None:
            await self.outcome_tracker.add(
                signal_id,
                symbol=d.symbol,
                entry_price=d.suggested_entry,
                suggested_sl=d.suggested_sl,
                suggested_tp1=d.suggested_tp1,
                suggested_tp2=d.suggested_tp2,
            )

    # ------------- introspection -------------

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "uptime_sec": int((now_ms() - self._started_at_ms) / 1000),
            "messages_seen": self._messages_seen,
            "signals_emitted": self._signals_emitted,
            "pumps_detected": self._pumps_detected,
            "symbols": len(self.symbols),
            "ws": self.stream_manager.stats(),
            "regime": self.regime.as_dict(),
            "anti_spam": self.anti_spam.to_dict(),
            "tracker": self.outcome_tracker.snapshot() if self.outcome_tracker else [],
        }


# ------------- helpers -------------


def _kline_from_rest(row: list[Any]) -> Kline:
    # Binance kline REST array: [open_time, open, high, low, close, volume,
    # close_time, quote_volume, trades, taker_buy_vol, taker_buy_quote_vol, ignore]
    return Kline(
        open_ms=int(row[0]),
        close_ms=int(row[6]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trades=int(row[8]),
        taker_buy_vol=float(row[9]),
        closed=True,
    )


async def run() -> None:
    """Run the engine until SIGINT/SIGTERM (or :func:`shutdown` is called)."""
    eng = Engine()
    await eng.start()
    try:
        await eng._stopping.wait()  # noqa: SLF001
    finally:
        await eng.shutdown()
