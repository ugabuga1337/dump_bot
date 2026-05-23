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
from connectors import BybitFuturesREST, StreamManager
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
        self.rest = BybitFuturesREST(
            self.settings.bybit.rest_url,
            category=self.settings.bybit.category,
        )
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
            self.settings.bybit.ws_url, self._on_ws_message
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
        # Force one event-loop turn so the freshly-created WS shard tasks
        # actually begin their ``websockets.connect`` await BEFORE the rest
        # of startup (telegram, watchdog, background loops) piles on. Without
        # this yield the shard tasks can sit pending while the snapshot /
        # OI loops claim every subsequent loop turn, and shards stay stuck
        # at ``connected: False`` indefinitely.
        await asyncio.sleep(0)
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
        # Bybit topics are upper-cased symbol-suffixed and use a dot separator.
        # publicTrade.<SYM>, kline.1.<SYM>, tickers.<SYM>
        streams: list[str] = []
        for s in symbols:
            sym = s.upper()
            streams.append(f"publicTrade.{sym}")
            streams.append(f"kline.1.{sym}")
            streams.append(f"tickers.{sym}")
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
        """Open interest doesn't have a free public WS — poll REST every 60s.

        Delay the first iteration so the freshly-launched WS shards get a
        clean window to finish their TLS + WS handshakes before this loop
        fans out ~120 concurrent REST calls into the same aiohttp pool.
        """
        # Give WS shards a head start on connecting; tickers stream will
        # also start pushing openInterest deltas long before this elapses.
        await asyncio.sleep(15)
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
        """Bybit v5 public envelope::

            {"topic": "<channel>.<...>.<SYMBOL>",
             "type": "snapshot" | "delta",
             "ts": 1700000000000,
             "data": ... }

        ``data`` is a list for publicTrade/kline and a dict for tickers.
        Tickers payloads are delta-merged so missing fields are unchanged.
        """
        self._messages_seen += 1
        topic = payload.get("topic")
        if not topic or not isinstance(topic, str):
            return
        data = payload.get("data")
        # Topic format: "<channel>.<...>.<SYMBOL>" — symbol is the last segment.
        parts = topic.split(".")
        if len(parts) < 2:
            return
        channel = parts[0]
        symbol = parts[-1].upper()

        st = self.state.get(symbol)
        if st is None:
            return

        try:
            if channel == "publicTrade":
                if not isinstance(data, list):
                    return
                last_trade: TradePrint | None = None
                for row in data:
                    side = row.get("S") or ""
                    # Bybit "S" is the taker's side. If the taker is a seller
                    # the buyer was the resting maker -> is_buyer_maker = True.
                    is_buyer_maker = side.upper() == "SELL"
                    t = TradePrint(
                        ts_ms=int(row.get("T") or 0),
                        price=float(row.get("p") or 0.0),
                        qty=float(row.get("v") or 0.0),
                        is_buyer_maker=is_buyer_maker,
                    )
                    if t.price <= 0 or t.qty <= 0:
                        continue
                    st.push_trade(t)
                    last_trade = t
                if (
                    last_trade is not None
                    and self.outcome_tracker is not None
                    and symbol in self.outcome_tracker.tracked_symbols()
                ):
                    await self.outcome_tracker.on_price(
                        symbol, last_trade.price, last_trade.ts_ms
                    )
            elif channel == "kline":
                if not isinstance(data, list):
                    return
                fire_evaluate = False
                for k_raw in data:
                    k = Kline(
                        open_ms=int(k_raw.get("start") or 0),
                        close_ms=int(k_raw.get("end") or 0),
                        open=float(k_raw.get("open") or 0.0),
                        high=float(k_raw.get("high") or 0.0),
                        low=float(k_raw.get("low") or 0.0),
                        close=float(k_raw.get("close") or 0.0),
                        volume=float(k_raw.get("volume") or 0.0),
                        quote_volume=float(k_raw.get("turnover") or 0.0),
                        # Bybit doesn't publish per-candle trade count or
                        # taker-buy split on public WS. Strategy already
                        # tolerates zeros (used as proxies, not invariants).
                        trades=0,
                        taker_buy_vol=0.0,
                        closed=bool(k_raw.get("confirm", False)),
                    )
                    st.push_kline(k)
                    if k.closed:
                        fire_evaluate = True
                if fire_evaluate:
                    await self._evaluate(symbol)
            elif channel == "tickers":
                if not isinstance(data, dict):
                    return
                prev = st.last_mark
                ts_ms = int(payload.get("ts") or now_ms())
                # Pull each field if present, fall back to prior tick.
                mark = data.get("markPrice")
                index = data.get("indexPrice")
                funding = data.get("fundingRate")
                next_funding = data.get("nextFundingTime")
                try:
                    mark_v = float(mark) if mark is not None else (
                        prev.mark_price if prev else 0.0
                    )
                    index_v = float(index) if index is not None else (
                        prev.index_price if prev else 0.0
                    )
                    funding_v = float(funding) if funding is not None else (
                        prev.funding_rate if prev else 0.0
                    )
                    next_v = int(next_funding) if next_funding is not None else (
                        prev.next_funding_ms if prev else 0
                    )
                except (TypeError, ValueError):
                    return
                if mark_v <= 0:
                    return
                st.push_mark(MarkPriceTick(
                    ts_ms=ts_ms,
                    mark_price=mark_v,
                    index_price=index_v,
                    funding_rate=funding_v,
                    next_funding_ms=next_v,
                ))
                # Bybit publishes openInterest on the tickers stream too —
                # it's cheaper to grab it here than to wait for the REST poll.
                oi_raw = data.get("openInterest")
                if oi_raw is not None:
                    try:
                        st.push_oi(ts_ms, float(oi_raw))
                    except (TypeError, ValueError):
                        pass
        except Exception as exc:  # noqa: BLE001
            log.debug("ws_payload_parse_failed",
                      extra={"err": repr(exc), "topic": topic})

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
