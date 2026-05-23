"""Tests for the Bybit WS message translator.

We don't open a real socket — ``BybitWSClient._translate`` is a pure
function over Bybit-shaped frames. The tests below feed it realistic
``publicTrade`` / ``kline.1`` / ``tickers`` payloads (snapshot + delta)
and assert it produces the Binance-shaped envelope the engine router
already parses.
"""

from __future__ import annotations

from typing import Any

from connectors.binance_ws import BybitWSClient, _split_topic


def make_client() -> BybitWSClient:
    async def _noop(_p: dict[str, Any]) -> None:  # pragma: no cover — never called here
        pass

    # Streams list must be non-empty per the constructor's contract.
    return BybitWSClient(
        "wss://stream.bybit.com/v5/public/linear",
        streams=["publicTrade.BTCUSDT"],
        on_message=_noop,
    )


def test_split_topic_handles_kline_three_part_name() -> None:
    assert _split_topic("publicTrade.BTCUSDT") == ("publicTrade", "BTCUSDT")
    assert _split_topic("kline.1.BTCUSDT") == ("kline", "BTCUSDT")
    assert _split_topic("tickers.BTCUSDT") == ("tickers", "BTCUSDT")


def test_publictrade_buy_side_marks_taker_as_buyer() -> None:
    client = make_client()
    out = client._translate({
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000000,
        "data": [{
            "T": 1700000000123,
            "s": "BTCUSDT",
            "S": "Buy",  # taker bought -> buyer is taker, NOT maker
            "v": "0.123",
            "p": "75000.5",
            "i": "trade-1",
        }],
    })
    assert len(out) == 1
    msg = out[0]
    assert msg["stream"] == "btcusdt@aggTrade"
    data = msg["data"]
    assert data["e"] == "aggTrade"
    assert data["s"] == "BTCUSDT"
    assert data["T"] == 1700000000123
    assert data["p"] == "75000.5"
    assert data["q"] == "0.123"
    assert data["m"] is False  # buyer is NOT maker when taker bought


def test_publictrade_sell_side_marks_buyer_as_maker() -> None:
    client = make_client()
    out = client._translate({
        "topic": "publicTrade.ETHUSDT",
        "ts": 1700000000000,
        "data": [{"T": 1, "s": "ETHUSDT", "S": "Sell", "v": "1", "p": "4000"}],
    })
    assert out[0]["data"]["m"] is True  # taker is seller -> buyer is maker


def test_publictrade_multiple_trades_expand_to_multiple_payloads() -> None:
    client = make_client()
    out = client._translate({
        "topic": "publicTrade.BTCUSDT",
        "ts": 1700000000000,
        "data": [
            {"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "0.1", "p": "75000"},
            {"T": 2, "s": "BTCUSDT", "S": "Sell", "v": "0.2", "p": "75001"},
            {"T": 3, "s": "BTCUSDT", "S": "Buy", "v": "0.3", "p": "75002"},
        ],
    })
    assert len(out) == 3
    assert [m["data"]["T"] for m in out] == [1, 2, 3]
    assert [m["data"]["m"] for m in out] == [False, True, False]


def test_kline_payload_translates_confirm_to_x_flag() -> None:
    client = make_client()
    out = client._translate({
        "topic": "kline.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000060000,
        "data": [{
            "start": 1700000000000,
            "end":   1700000059999,
            "interval": "1",
            "open":   "75000.0",
            "high":   "75100.0",
            "low":    "74900.0",
            "close":  "75050.0",
            "volume": "12.34",
            "turnover": "925000.0",
            "confirm": True,
            "timestamp": 1700000060000,
        }],
    })
    assert len(out) == 1
    msg = out[0]
    assert msg["stream"] == "btcusdt@kline_1m"
    data = msg["data"]
    assert data["e"] == "kline"
    assert data["s"] == "BTCUSDT"
    k = data["k"]
    assert k["t"] == 1700000000000
    assert k["T"] == 1700000059999
    assert k["o"] == "75000.0"
    assert k["c"] == "75050.0"
    assert k["v"] == "12.34"
    assert k["q"] == "925000.0"      # turnover -> quote vol
    assert k["x"] is True            # confirm -> Binance ``x`` (closed flag)
    # Trade count / taker-buy fields aren't published on Bybit kline WS;
    # placeholders keep the parser in the engine happy.
    assert k["n"] == 0
    assert k["V"] == "0"


def test_tickers_snapshot_emits_markprice_payload() -> None:
    client = make_client()
    out = client._translate({
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
            "openInterest": "1234.5",
        },
    })
    assert len(out) == 1
    msg = out[0]
    assert msg["stream"] == "btcusdt@markPrice"
    data = msg["data"]
    assert data["e"] == "markPriceUpdate"
    assert data["s"] == "BTCUSDT"
    assert float(data["p"]) == 75001.5
    assert float(data["i"]) == 75000.7
    assert float(data["r"]) == 0.000123
    assert data["T"] == 1700003600000


def test_tickers_delta_merges_into_snapshot_cache() -> None:
    client = make_client()
    # Snapshot seeds the cache.
    client._translate({
        "topic": "tickers.ETHUSDT",
        "type": "snapshot",
        "ts": 1700000000000,
        "data": {
            "symbol": "ETHUSDT",
            "markPrice": "4000",
            "indexPrice": "4001",
            "fundingRate": "0.0001",
            "nextFundingTime": "1700003600000",
        },
    })
    # Delta only carries fundingRate; markPrice / indexPrice must persist.
    out = client._translate({
        "topic": "tickers.ETHUSDT",
        "type": "delta",
        "ts": 1700000005000,
        "data": {"symbol": "ETHUSDT", "fundingRate": "-0.0002"},
    })
    assert len(out) == 1
    data = out[0]["data"]
    assert float(data["p"]) == 4000.0       # cached markPrice survived
    assert float(data["r"]) == -0.0002      # delta applied


def test_tickers_delta_before_snapshot_is_treated_as_snapshot() -> None:
    """If we somehow receive a delta first, treat it as a seed instead of
    failing — Bybit occasionally reconnects mid-stream and the first frame
    on the new socket can be tagged "delta"."""
    client = make_client()
    out = client._translate({
        "topic": "tickers.SOLUSDT",
        "type": "delta",
        "ts": 1,
        "data": {"symbol": "SOLUSDT", "markPrice": "100", "indexPrice": "100.1",
                 "fundingRate": "0.0001", "nextFundingTime": "0"},
    })
    assert len(out) == 1
    assert float(out[0]["data"]["p"]) == 100.0


def test_tickers_delta_without_markprice_yields_nothing() -> None:
    """When we have no cached markPrice yet, a delta missing markPrice must
    not be emitted — downstream code would parse ``p`` as float and fail."""
    client = make_client()
    out = client._translate({
        "topic": "tickers.NEWUSDT",
        "type": "delta",
        "ts": 1,
        "data": {"symbol": "NEWUSDT", "fundingRate": "0.0001"},
    })
    assert out == []


def test_subscribe_failure_is_logged_not_dispatched() -> None:
    client = make_client()
    out = client._translate(
        {"op": "subscribe", "success": False, "ret_msg": "invalid topic"}
    )
    assert out == []


def test_pong_frames_are_dropped() -> None:
    client = make_client()
    assert client._translate({"op": "pong"}) == []
    # Bybit sometimes responds with success=True + ret_msg="pong" instead.
    assert client._translate({"success": True, "ret_msg": "pong"}) == []


def test_unknown_topic_is_ignored_safely() -> None:
    client = make_client()
    assert client._translate({"topic": "orderbook.50.BTCUSDT", "data": []}) == []
    assert client._translate({"garbage": "value"}) == []
    assert client._translate("not even a dict") == []  # type: ignore[arg-type]
