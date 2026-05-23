"""Bybit Futures (USDT linear perpetuals) public REST client.

Read-only — only public endpoints under ``/v5/market/*`` are used. No API key,
no signing, no risk of accidental order placement.

Method names and return shapes are kept compatible with the legacy Binance
adapter so the engine, universe, regime, and backtester don't need to learn
the Bybit wire format. Where a field doesn't have a direct Bybit equivalent
(e.g. taker_buy_vol in REST klines, trade count), a neutral placeholder is
returned rather than raising — keeps downstream features that lean on those
fields degraded but functional.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

log = logging.getLogger("bybit.rest")


# Bybit kline interval → milliseconds. Used to synthesize close_time so the
# returned rows match the Binance kline array shape (which carries both
# open_time and close_time).
_INTERVAL_MS: dict[str, int] = {
    "1": 60_000,
    "3": 180_000,
    "5": 300_000,
    "15": 900_000,
    "30": 1_800_000,
    "60": 3_600_000,
    "120": 7_200_000,
    "240": 14_400_000,
    "360": 21_600_000,
    "720": 43_200_000,
    "D": 86_400_000,
    "W": 604_800_000,
    "M": 30 * 86_400_000,  # rough — only used for close_time synthesis
}

# Map Binance-style intervals ("1m", "1h", "1d") onto Bybit's interval codes.
_INTERVAL_MAP: dict[str, str] = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W", "1M": "M",
}


def _to_bybit_interval(interval: str) -> str:
    if interval in _INTERVAL_MAP:
        return _INTERVAL_MAP[interval]
    # Already a Bybit code (e.g. caller passed "60")
    return interval


class BybitRestError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


# Legacy alias — keep import paths from the previous Binance adapter working.
BinanceRestError = BybitRestError


class BybitFuturesREST:
    """Aiohttp-backed REST client with retries, rate limit handling, and
    a shared connector pool kept small to fit a 1GB VPS."""

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        *,
        timeout: float = 10.0,
        max_retries: int = 4,
        user_agent: str = "dump_bot/0.1",
        category: str = "linear",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self._session: aiohttp.ClientSession | None = None
        self._user_agent = user_agent
        self._category = category
        self._closed = False

    async def __aenter__(self) -> BybitFuturesREST:
        await self._ensure_session()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=8, limit_per_host=8, ttl_dns_cache=300, enable_cleanup_closed=True
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers={"User-Agent": self._user_agent, "Accept": "application/json"},
                raise_for_status=False,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._closed = True

    async def _request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        method: str = "GET",
    ) -> Any:
        session = await self._ensure_session()
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            attempt += 1
            try:
                async with session.request(method, url, params=params) as resp:
                    if resp.status == 429 or resp.status == 418:
                        retry = float(resp.headers.get("Retry-After", "5"))
                        log.warning(
                            "bybit_rate_limited",
                            extra={"path": path, "status": resp.status, "retry": retry},
                        )
                        await asyncio.sleep(min(30.0, retry))
                        continue
                    if 500 <= resp.status < 600:
                        text = await resp.text()
                        log.warning("bybit_5xx",
                                    extra={"path": path, "status": resp.status,
                                           "body": text[:200]})
                        if attempt > self.max_retries:
                            raise BybitRestError(resp.status, text)
                        await asyncio.sleep(min(15.0, 0.5 * (2 ** attempt)))
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        raise BybitRestError(resp.status, text)
                    body = await resp.json(loads=_loads)
                    # Bybit packages business-level errors in retCode != 0.
                    if isinstance(body, dict) and body.get("retCode", 0) not in (0, None):
                        msg = body.get("retMsg") or "unknown"
                        raise BybitRestError(resp.status, f"retCode={body['retCode']} {msg}")
                    return body
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt > self.max_retries:
                    raise BybitRestError(0, f"network error: {exc}") from exc
                wait = min(15.0, 0.5 * (2 ** attempt))
                log.warning("bybit_net_error",
                            extra={"path": path, "attempt": attempt, "wait": wait,
                                   "err": repr(exc)})
                await asyncio.sleep(wait)

    # ---------- Public endpoints ----------

    async def exchange_info(self) -> dict[str, Any]:
        """Return instruments shaped like Binance ``exchangeInfo``.

        Each row carries the fields ``universe.py`` actually reads:
        ``symbol``, ``contractType`` (normalized to ``PERPETUAL`` for linear
        perpetuals), ``status`` (normalized to ``TRADING``), ``baseAsset``,
        ``quoteAsset``.
        """
        raw = await self._request(
            "/v5/market/instruments-info", params={"category": self._category}
        )
        rows = (raw.get("result") or {}).get("list") or []
        out: list[dict[str, Any]] = []
        for r in rows:
            contract = (r.get("contractType") or "").lower()
            status = (r.get("status") or "").lower()
            # Only linear perpetuals (Bybit uses "LinearPerpetual").
            ct_norm = "PERPETUAL" if "perpetual" in contract else contract.upper()
            st_norm = "TRADING" if status == "trading" else status.upper()
            out.append({
                "symbol": (r.get("symbol") or "").upper(),
                "contractType": ct_norm,
                "status": st_norm,
                "baseAsset": (r.get("baseCoin") or "").upper(),
                "quoteAsset": (r.get("quoteCoin") or "").upper(),
            })
        return {"symbols": out}

    async def ticker_24h(self) -> list[dict[str, Any]]:
        """Return 24h tickers shaped like Binance.

        Output rows have ``symbol``, ``lastPrice``, ``quoteVolume`` (mapped
        from Bybit ``turnover24h``), ``priceChangePercent`` (mapped from
        ``price24hPcnt``, which Bybit returns as a decimal fraction).
        """
        raw = await self._request(
            "/v5/market/tickers", params={"category": self._category}
        )
        rows = (raw.get("result") or {}).get("list") or []
        out: list[dict[str, Any]] = []
        for t in rows:
            try:
                pct = float(t.get("price24hPcnt") or 0.0) * 100.0
            except (TypeError, ValueError):
                pct = 0.0
            out.append({
                "symbol": t.get("symbol") or "",
                "lastPrice": t.get("lastPrice") or "0",
                "quoteVolume": t.get("turnover24h") or "0",
                "volume": t.get("volume24h") or "0",
                "priceChangePercent": f"{pct:.6f}",
                "highPrice": t.get("highPrice24h") or "0",
                "lowPrice": t.get("lowPrice24h") or "0",
            })
        return out

    async def oi(self, symbol: str) -> dict[str, Any]:
        """Latest open interest for ``symbol``.

        Returns ``{"symbol", "openInterest", "time"}`` — same keys the engine
        already reads from the previous adapter.
        """
        raw = await self._request(
            "/v5/market/open-interest",
            params={
                "category": self._category,
                "symbol": symbol,
                "intervalTime": "5min",
                "limit": 1,
            },
        )
        rows = (raw.get("result") or {}).get("list") or []
        if not rows:
            return {"symbol": symbol, "openInterest": "0", "time": 0}
        row = rows[0]
        return {
            "symbol": symbol,
            "openInterest": row.get("openInterest") or "0",
            "time": int(row.get("timestamp") or 0),
        }

    # Legacy alias for callers that still use the Binance method name.
    async def open_interest(self, symbol: str) -> dict[str, Any]:
        return await self.oi(symbol)

    async def funding_rate(
        self, symbol: str | None = None, limit: int = 1
    ) -> list[dict[str, Any]]:
        """Latest funding rate history.

        Returns Binance-shaped rows: ``{"symbol", "fundingRate", "fundingTime"}``.
        Bybit requires a symbol on this endpoint (no global feed), so when
        called without one we just return an empty list — matches the
        "no data" semantics callers expected.
        """
        if not symbol:
            return []
        raw = await self._request(
            "/v5/market/funding/history",
            params={
                "category": self._category,
                "symbol": symbol,
                "limit": max(1, min(int(limit), 200)),
            },
        )
        rows = (raw.get("result") or {}).get("list") or []
        return [
            {
                "symbol": r.get("symbol") or symbol,
                "fundingRate": r.get("fundingRate") or "0",
                "fundingTime": int(r.get("fundingRateTimestamp") or 0),
            }
            for r in rows
        ]

    async def klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[list[Any]]:
        """1m (or other) klines, returned in Binance array layout.

        Bybit's ``/v5/market/kline`` returns the newest candle first as
        ``[startTime, open, high, low, close, volume, turnover]``. We reverse
        into chronological order and pad to the 12-field Binance shape the
        engine already parses. Per-candle trade count and taker-buy volumes
        aren't published on this endpoint, so positions 8/9/10 are zero.
        """
        bybit_interval = _to_bybit_interval(interval)
        params: dict[str, Any] = {
            "category": self._category,
            "symbol": symbol,
            "interval": bybit_interval,
            "limit": max(1, min(int(limit), 1000)),  # Bybit kline cap is 1000
        }
        if start_time is not None:
            params["start"] = int(start_time)
        if end_time is not None:
            params["end"] = int(end_time)
        raw = await self._request("/v5/market/kline", params=params)
        rows = (raw.get("result") or {}).get("list") or []
        bar_ms = _INTERVAL_MS.get(bybit_interval, 60_000)
        out: list[list[Any]] = []
        # Bybit returns newest first — flip so the consumer sees oldest first.
        for r in reversed(rows):
            try:
                open_ms = int(r[0])
            except (TypeError, ValueError, IndexError):
                continue
            out.append([
                open_ms,                    # 0: open time
                r[1],                       # 1: open
                r[2],                       # 2: high
                r[3],                       # 3: low
                r[4],                       # 4: close
                r[5],                       # 5: base volume
                open_ms + bar_ms - 1,       # 6: close time
                r[6] if len(r) > 6 else "0",  # 7: quote volume (turnover)
                0,                          # 8: trades (not available)
                "0",                        # 9: taker_buy_vol (not available)
                "0",                        # 10: taker_buy_quote (not available)
                "0",                        # 11: ignore
            ])
        return out


# Legacy alias so ``from connectors.binance_rest import BinanceFuturesREST``
# still resolves while the rest of the codebase migrates to the new name.
BinanceFuturesREST = BybitFuturesREST


def _loads(s: bytes | str) -> Any:
    try:
        import orjson  # type: ignore

        if isinstance(s, str):
            s = s.encode()
        return orjson.loads(s)
    except ImportError:  # pragma: no cover
        import json as _j

        if isinstance(s, bytes):
            s = s.decode()
        return _j.loads(s)
