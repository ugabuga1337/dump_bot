"""Unit tests for the Bybit v5 REST adapter.

We don't hit the network — the public ``_request`` is patched to return
canned Bybit responses, and we verify the public methods normalize the
result into the Binance-shaped contracts the rest of the engine reads.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.binance_rest import BybitFuturesREST


@pytest.fixture
def rest(monkeypatch: pytest.MonkeyPatch) -> BybitFuturesREST:
    client = BybitFuturesREST()
    responses: dict[tuple[str, frozenset], Any] = {}

    async def fake_request(
        path: str,
        *,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> Any:
        key = (path, frozenset((params or {}).items()))
        # Allow path-only lookup as a fallback so tests can pin behavior by
        # endpoint without caring about every query-string permutation.
        if key in responses:
            return responses[key]
        for (p, _), payload in responses.items():
            if p == path:
                return payload
        raise AssertionError(f"unexpected request to {path} with {params}")

    monkeypatch.setattr(client, "_request", fake_request)
    client._fake_responses = responses  # type: ignore[attr-defined]
    return client


async def test_ticker_24h_maps_turnover_and_price_change(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/tickers", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "lastPrice": "75000",
                    "turnover24h": "123456789",
                    "volume24h": "1234.5",
                    "price24hPcnt": "0.0512",  # decimal fraction
                    "highPrice24h": "76000",
                    "lowPrice24h": "73000",
                },
                {
                    "symbol": "ETHUSDT",
                    "lastPrice": "4000",
                    "turnover24h": "9876543",
                    "volume24h": "100.0",
                    "price24hPcnt": "-0.0237",
                },
            ]
        },
    }

    rows = await rest.ticker_24h()

    assert len(rows) == 2
    btc = rows[0]
    assert btc["symbol"] == "BTCUSDT"
    assert btc["lastPrice"] == "75000"
    assert btc["quoteVolume"] == "123456789"  # mapped from turnover24h
    # priceChangePercent should be the bybit fraction × 100
    assert float(btc["priceChangePercent"]) == pytest.approx(5.12, rel=1e-4)
    eth = rows[1]
    assert float(eth["priceChangePercent"]) == pytest.approx(-2.37, rel=1e-4)


async def test_ticker_24h_handles_garbage_price_change(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/tickers", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {"list": [{
            "symbol": "FOO",
            "lastPrice": "1",
            "turnover24h": "0",
            "price24hPcnt": "not-a-number",
        }]},
    }
    rows = await rest.ticker_24h()
    assert rows[0]["priceChangePercent"] == "0.000000"


async def test_exchange_info_normalizes_linear_perpetuals(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/instruments-info", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {"list": [
            {
                "symbol": "BTCUSDT",
                "contractType": "LinearPerpetual",
                "status": "Trading",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
            },
            {
                "symbol": "SOLUSDC",
                "contractType": "LinearFutures",  # NOT a perpetual
                "status": "Trading",
                "baseCoin": "SOL",
                "quoteCoin": "USDC",
            },
            {
                "symbol": "OLDUSDT",
                "contractType": "LinearPerpetual",
                "status": "Closed",
                "baseCoin": "OLD",
                "quoteCoin": "USDT",
            },
        ]},
    }
    info = await rest.exchange_info()
    by_symbol = {r["symbol"]: r for r in info["symbols"]}

    btc = by_symbol["BTCUSDT"]
    assert btc["contractType"] == "PERPETUAL"  # the engine filters on this token
    assert btc["status"] == "TRADING"
    assert btc["quoteAsset"] == "USDT"
    assert btc["baseAsset"] == "BTC"

    # Non-perpetual stays distinguishable so universe filtering drops it.
    assert by_symbol["SOLUSDC"]["contractType"] != "PERPETUAL"
    # Closed instrument should not have the "TRADING" sentinel.
    assert by_symbol["OLDUSDT"]["status"] != "TRADING"


async def test_oi_returns_engine_compatible_shape(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/open-interest", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {"list": [
            {"openInterest": "12345.67", "timestamp": "1700000000000"},
        ], "symbol": "BTCUSDT"},
    }
    row = await rest.oi("BTCUSDT")
    assert row["symbol"] == "BTCUSDT"
    assert float(row["openInterest"]) == pytest.approx(12345.67)
    assert row["time"] == 1700000000000


async def test_oi_empty_list_returns_zero_placeholder(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/open-interest", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {"list": []},
    }
    row = await rest.oi("BTCUSDT")
    assert row["openInterest"] == "0"
    assert row["time"] == 0


async def test_funding_rate_returns_list_with_int_time(rest: BybitFuturesREST) -> None:
    rest._fake_responses[("/v5/market/funding/history", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {"list": [
            {
                "symbol": "BTCUSDT",
                "fundingRate": "0.0001",
                "fundingRateTimestamp": "1700000000000",
            },
        ]},
    }
    rows = await rest.funding_rate("BTCUSDT")
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["fundingRate"] == "0.0001"
    assert isinstance(rows[0]["fundingTime"], int)
    assert rows[0]["fundingTime"] == 1700000000000


async def test_funding_rate_no_symbol_returns_empty() -> None:
    # Bybit requires a symbol on this endpoint — calling without one shouldn't
    # raise, just return an empty list so callers can default to "no data".
    client = BybitFuturesREST()
    assert await client.funding_rate(None) == []


async def test_klines_reverses_and_pads_to_binance_shape(
    rest: BybitFuturesREST,
) -> None:
    # Bybit returns klines newest-first as 7-element arrays.
    rest._fake_responses[("/v5/market/kline", frozenset())] = {  # type: ignore[attr-defined]
        "retCode": 0,
        "result": {
            "category": "linear",
            "symbol": "BTCUSDT",
            "list": [
                # newest first
                ["1700000180000", "102", "103", "101", "102.5", "10.0", "1025.0"],
                ["1700000120000", "101", "102", "100", "101.5", "12.0", "1218.0"],
                ["1700000060000", "100", "101",  "99", "100.5",  "8.0",  "804.0"],
            ],
        },
    }

    rows = await rest.klines("BTCUSDT", interval="1m", limit=3)

    # Expect oldest-first (Binance order)
    assert [r[0] for r in rows] == [
        1700000060000, 1700000120000, 1700000180000,
    ]
    # 12-field Binance-shape rows
    assert all(len(r) == 12 for r in rows)
    # Open/high/low/close/volume preserved as strings (engine casts to float).
    assert rows[0][1:6] == ["100", "101", "99", "100.5", "8.0"]
    # close_time = open_time + 60s - 1 ms for the 1m interval
    assert rows[0][6] == 1700000060000 + 60_000 - 1
    # quote volume (turnover) at position 7
    assert rows[0][7] == "804.0"
    # trades / taker-buy fields are placeholders since Bybit doesn't publish them
    assert rows[0][8] == 0
    assert rows[0][9] == "0"


async def test_klines_interval_mapping(rest: BybitFuturesREST) -> None:
    captured: dict[str, Any] = {}

    async def fake_request(path, *, params=None, method="GET"):  # type: ignore[no-untyped-def]
        captured["params"] = params
        return {"retCode": 0, "result": {"list": []}}

    # Override the previous fixture stub
    rest._request = fake_request  # type: ignore[method-assign]
    await rest.klines("BTCUSDT", interval="1h", limit=5)
    assert captured["params"]["interval"] == "60"
    await rest.klines("BTCUSDT", interval="1m", limit=5)
    assert captured["params"]["interval"] == "1"
    await rest.klines("BTCUSDT", interval="1d", limit=5)
    assert captured["params"]["interval"] == "D"
