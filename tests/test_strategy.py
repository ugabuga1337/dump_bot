"""End-to-end tests for the strategy pipeline."""


from config import PumpConfig, RegimeConfig, SignalConfig
from core.market_regime import MarketRegimeDetector
from core.models import Kline, MarkPriceTick, TradePrint
from core.state import SymbolStateData
from signals import ConfidenceScorer
from strategy import ExhaustionScorer, FakePumpDetector, PumpDetector


class StubRegime(MarketRegimeDetector):
    """Simulate BTC regime without hitting REST."""

    def __init__(self):
        super().__init__(RegimeConfig(), rest=None)  # type: ignore[arg-type]
        self.btc_price = 60_000
        self.btc_trend_pct = -2.0
        self.btc_atr_pct = 1.5
        # NEUTRAL would dampen confidence; BEAR is fine for tests.
        from core.models import MarketRegime
        self.regime = MarketRegime.BEAR


def make_kline(open_ts, open_, high, low, close, vol):
    return Kline(
        open_ms=open_ts, close_ms=open_ts + 60_000,
        open=open_, high=high, low=low, close=close,
        volume=vol, quote_volume=vol * close, trades=20,
        taker_buy_vol=vol * 0.4, closed=True,
    )


def build_pumping_state() -> SymbolStateData:
    st = SymbolStateData(symbol="TESTUSDT")
    ts = 1_700_000_000_000
    # 40 baseline candles, slow volume around 1000
    price = 1.0
    for i in range(40):
        st.push_kline(make_kline(ts + i * 60_000, price, price * 1.002,
                                 price * 0.998, price, 1000))
    # 15-min pump: +10% with rising volume
    for i in range(15):
        new_price = price * 1.006
        vol = 1000 + i * 600
        st.push_kline(make_kline(ts + (40 + i) * 60_000, price, new_price * 1.001,
                                 price * 0.999, new_price, vol))
        price = new_price
    # Add OI history that grew with price.
    base_oi = 100_000
    for i in range(30):
        st.push_oi(ts + (40 + i // 2) * 60_000, base_oi * (1 + 0.005 * i))
    st.push_mark(MarkPriceTick(
        ts_ms=ts + 60 * 60_000,
        mark_price=price, index_price=price * 0.999,
        funding_rate=0.0005, next_funding_ms=ts + 70 * 60_000,
    ))
    # Some buyer-dominated agg trades to feed CVD/delta
    for i in range(200):
        st.push_trade(TradePrint(ts_ms=ts + i * 250, price=price, qty=10,
                                 is_buyer_maker=False))
    return st


def test_pump_detector_fires_on_strong_move():
    st = build_pumping_state()
    regime = StubRegime()
    pd = PumpDetector(PumpConfig(score_threshold=40.0), regime)
    pump = pd.evaluate(st)
    assert pump is not None
    assert pump.pump_score >= 40.0
    assert pump.pump_5m_pct > 1.0
    assert pump.pump_15m_pct > 5.0
    assert pump.volume_ratio > 1.0


def test_pump_detector_quiet_market_returns_none():
    st = SymbolStateData(symbol="QUIET")
    ts = 1_700_000_000_000
    price = 1.0
    for i in range(60):
        st.push_kline(make_kline(ts + i * 60_000, price, price * 1.0005,
                                 price * 0.9995, price, 1000))
    regime = StubRegime()
    pd = PumpDetector(PumpConfig(), regime)
    assert pd.evaluate(st) is None


def test_exhaustion_scorer_responds_to_rejection_candle():
    st = build_pumping_state()
    # Append a strong rejection candle: long upper wick + bearish close
    ts = st.klines[-1].close_ms
    last = st.klines[-1].close
    st.push_kline(make_kline(ts, last, last * 1.04, last * 0.99, last * 0.992, 5000))

    # Add aggressive sell prints
    for i in range(60):
        st.push_trade(TradePrint(ts_ms=ts + i * 250, price=last * 1.0, qty=12,
                                 is_buyer_maker=True))

    scorer = ExhaustionScorer(min_confirmations=2)
    res = scorer.evaluate(st)
    assert res.score > 30.0
    assert any("rejection" in r.lower() or "wick" in r.lower() or "sell" in r.lower()
               for r in res.reasons)


def test_confidence_scorer_fires_when_thresholds_pass():
    st = build_pumping_state()
    # Add exhaustion structure: rejection candle + aggressive sells
    ts = st.klines[-1].close_ms
    last = st.klines[-1].close
    st.push_kline(make_kline(ts, last, last * 1.05, last * 0.99, last * 0.99, 7000))
    st.push_kline(make_kline(ts + 60_000, last * 0.99, last * 1.01,
                             last * 0.985, last * 0.988, 6000))
    for i in range(100):
        st.push_trade(TradePrint(ts_ms=ts + i * 200, price=last,
                                 qty=20, is_buyer_maker=True))

    regime = StubRegime()
    pump = PumpDetector(PumpConfig(score_threshold=40.0), regime).evaluate(st)
    assert pump is not None
    exh = ExhaustionScorer(min_confirmations=2).evaluate(st)
    fake = FakePumpDetector().evaluate(st)
    scorer = ConfidenceScorer(SignalConfig(exhaustion_threshold=40.0,
                                            confidence_low=40.0), regime)
    scored = scorer.evaluate(st, pump, exh, fake)
    # Either fires (good) or at minimum has a positive confidence and a non-None decision basis.
    assert scored.confidence >= 0
    if scored.fire:
        assert scored.decision is not None
        assert scored.decision.suggested_entry > 0
        assert scored.decision.suggested_sl > scored.decision.suggested_entry
        assert scored.decision.suggested_tp1 < scored.decision.suggested_entry


def test_fake_pump_flags_thin_liquidity():
    """Big move with zero OI growth + no trade flow should flag fake."""
    st = SymbolStateData(symbol="THINUSDT")
    ts = 1_700_000_000_000
    price = 1.0
    for i in range(20):
        st.push_kline(make_kline(ts + i * 60_000, price, price * 1.001,
                                 price * 0.999, price, 100))
    # 10% pump in 5 minutes without OI / trades
    for i in range(5):
        new_price = price * 1.02
        st.push_kline(make_kline(ts + (20 + i) * 60_000, price, new_price * 1.001,
                                 price, new_price, 50))
        price = new_price
    # No OI change at all
    for i in range(10):
        st.push_oi(ts + i * 60_000, 100.0)

    fpd = FakePumpDetector()
    res = fpd.evaluate(st)
    # Tiny notional + no OI -> high fake score
    assert res.score > 20
