"""Tests for the per-symbol state container."""

import time

from core.models import Kline, MarkPriceTick, TradePrint
from core.state import SymbolStateData


def make_kline(open_ts, open_, high, low, close, vol, closed=True):
    return Kline(
        open_ms=open_ts, close_ms=open_ts + 60_000,
        open=open_, high=high, low=low, close=close,
        volume=vol, quote_volume=vol * close, trades=10,
        taker_buy_vol=vol / 2, closed=closed,
    )


def test_push_kline_streaming_stats():
    st = SymbolStateData(symbol="TEST")
    ts = 1700000000000
    for i in range(20):
        st.push_kline(make_kline(ts + i * 60_000, 100 + i, 101 + i, 99 + i,
                                 100 + i + 0.5, 1000 + i * 10))
    assert len(st.klines) == 20
    assert len(st.closes_1m) == 20
    assert st.last_price() > 100
    assert st.volumes_1m.mean > 1000


def test_recent_return_pct_positive_pump():
    st = SymbolStateData(symbol="TEST")
    ts = 1700000000000
    # 30 candles flat at 100, then 5% pump in last 5 candles
    for i in range(30):
        st.push_kline(make_kline(ts + i * 60_000, 100, 101, 99, 100, 1000))
    for i in range(30, 35):
        price = 100 + (i - 29)  # 101..105
        st.push_kline(make_kline(ts + i * 60_000, price, price + 0.5,
                                 price - 0.5, price, 1500))
    ret_5m = st.recent_return_pct(5)
    assert ret_5m > 4.0
    assert ret_5m < 6.0


def test_volume_ratio_spike():
    st = SymbolStateData(symbol="TEST")
    ts = 1700000000000
    for i in range(30):
        st.push_kline(make_kline(ts + i * 60_000, 100, 101, 99, 100, 1000))
    for i in range(30, 35):
        st.push_kline(make_kline(ts + i * 60_000, 100 + i - 30, 101, 99, 100, 5000))
    r = st.recent_volume_ratio(5)
    assert r > 3.0


def test_cvd_tracks_taker_sign():
    st = SymbolStateData(symbol="TEST")
    # Buyer aggressors (m=False) -> positive notional
    for i in range(5):
        st.push_trade(TradePrint(ts_ms=1000 + i, price=100, qty=1.0,
                                 is_buyer_maker=False))
    assert st.cvd == 500.0
    # Seller aggressors push CVD down
    for i in range(5):
        st.push_trade(TradePrint(ts_ms=2000 + i, price=100, qty=1.0,
                                 is_buyer_maker=True))
    assert st.cvd == 0.0


def test_mark_price_and_funding():
    st = SymbolStateData(symbol="TEST")
    st.push_mark(MarkPriceTick(
        ts_ms=int(time.time() * 1000), mark_price=100.0, index_price=99.9,
        funding_rate=0.0004, next_funding_ms=int(time.time() * 1000) + 3_600_000,
    ))
    assert st.last_mark is not None
    assert st.last_mark.funding_rate == 0.0004
