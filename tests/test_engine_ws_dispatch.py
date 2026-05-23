"""End-to-end check that Bybit WS frames reach engine state.

This is the path that previously broke: WS connector receives a Bybit frame,
translates it, then ``Engine._on_ws_message`` parses the translated envelope
and pushes into the per-symbol state. If any link is wrong the symbol's
state stays empty even when frames flow.

We instantiate ``Engine`` but never call ``start()`` — no DB, no aiohttp
session, no real socket. Just verify the message router.
"""

from __future__ import annotations

import pytest

from connectors.binance_ws import BybitWSClient
from core.engine import Engine
from core.state import SymbolStateData


def _make_translator() -> BybitWSClient:
    async def _noop(_p):  # pragma: no cover
        pass

    return BybitWSClient(
        "wss://stream.bybit.com/v5/public/linear",
        streams=["publicTrade.BTCUSDT"],
        on_message=_noop,
    )


@pytest.fixture
def engine() -> Engine:
    eng = Engine()
    # The engine routes by ``self.state[symbol]`` — preload an entry so the
    # message router doesn't drop our test frames.
    eng.state["BTCUSDT"] = SymbolStateData(symbol="BTCUSDT")
    eng.symbols = ["BTCUSDT"]
    return eng


async def test_bybit_publictrade_lands_in_state(engine: Engine) -> None:
    translator = _make_translator()
    frame = {
        "topic": "publicTrade.BTCUSDT",
        "ts": 1700000000000,
        "type": "snapshot",
        "data": [{
            "T": 1700000000050,
            "s": "BTCUSDT",
            "S": "Buy",
            "v": "0.250",
            "p": "75000",
        }],
    }
    for payload in translator._translate(frame):
        await engine._on_ws_message(payload)

    st = engine.state["BTCUSDT"]
    assert len(st.trades) == 1
    trade = st.trades[-1]
    assert trade.price == 75000.0
    assert trade.qty == 0.250
    # Buy taker -> buyer is NOT maker
    assert trade.is_buyer_maker is False
    # CVD should have moved positive (buyer aggressor)
    assert st.cvd > 0


async def test_bybit_kline_lands_in_state(engine: Engine) -> None:
    translator = _make_translator()
    frame = {
        "topic": "kline.1.BTCUSDT",
        "ts": 1700000060000,
        "type": "snapshot",
        "data": [{
            "start": 1700000000000,
            "end": 1700000059999,
            "interval": "1",
            "open": "74900",
            "high": "75100",
            "low": "74800",
            "close": "75000",
            "volume": "12.5",
            "turnover": "937500",
            "confirm": False,         # not closed -> engine won't run _evaluate
            "timestamp": 1700000060000,
        }],
    }
    for payload in translator._translate(frame):
        await engine._on_ws_message(payload)

    st = engine.state["BTCUSDT"]
    assert len(st.klines) == 1
    k = st.klines[-1]
    assert k.open == 74900
    assert k.close == 75000
    assert k.quote_volume == 937500
    assert k.closed is False


async def test_bybit_tickers_lands_in_state(engine: Engine) -> None:
    translator = _make_translator()
    frame = {
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000000,
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "75000",
            "markPrice": "75001.5",
            "indexPrice": "75000.7",
            "fundingRate": "0.000123",
            "nextFundingTime": "1700003600000",
        },
    }
    for payload in translator._translate(frame):
        await engine._on_ws_message(payload)

    st = engine.state["BTCUSDT"]
    assert st.last_mark is not None
    assert st.last_mark.mark_price == pytest.approx(75001.5)
    assert st.last_mark.index_price == pytest.approx(75000.7)
    assert st.last_mark.funding_rate == pytest.approx(0.000123)
    assert st.last_mark.next_funding_ms == 1700003600000


async def test_bybit_tickers_delta_after_snapshot_updates_funding(
    engine: Engine,
) -> None:
    translator = _make_translator()
    # Seed with snapshot.
    for payload in translator._translate({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1,
        "data": {
            "symbol": "BTCUSDT", "markPrice": "75000",
            "indexPrice": "75000", "fundingRate": "0.0001",
            "nextFundingTime": "0",
        },
    }):
        await engine._on_ws_message(payload)

    # Delta carrying only fundingRate must merge with cached fields.
    for payload in translator._translate({
        "topic": "tickers.BTCUSDT",
        "type": "delta",
        "ts": 2,
        "data": {"symbol": "BTCUSDT", "fundingRate": "-0.0005"},
    }):
        await engine._on_ws_message(payload)

    st = engine.state["BTCUSDT"]
    assert st.last_mark is not None
    assert st.last_mark.funding_rate == pytest.approx(-0.0005)
    assert st.last_mark.mark_price == pytest.approx(75000)


async def test_unknown_symbol_is_dropped_not_raised(engine: Engine) -> None:
    translator = _make_translator()
    # Symbol the engine isn't tracking — engine should ignore quietly.
    for payload in translator._translate({
        "topic": "publicTrade.UNKNOWNUSDT",
        "ts": 1,
        "data": [{"T": 1, "s": "UNKNOWNUSDT", "S": "Buy", "v": "1", "p": "1"}],
    }):
        await engine._on_ws_message(payload)

    assert "UNKNOWNUSDT" not in engine.state
    # messages_seen still increments because we received the frame.
    assert engine._messages_seen >= 1
