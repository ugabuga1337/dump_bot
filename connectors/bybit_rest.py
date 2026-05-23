"""Bybit v5 public REST client (USDT linear perpetuals).

Read-only — only public market-data endpoints are used. No API key, no
signing. The client normalizes Bybit's response shapes back to the
Binance-style dicts/arrays that the rest of the engine already consumes,
so transport can be swapped without touching the strategy code.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

log = logging.getLogger("bybit.rest")


# Translate the kline interval strings used by the engine ("1m", "1h", ...)
# to the integer-minutes / capital-letter codes Bybit expects.
_INTERVAL_MAP: dict[str, str] = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W", "1M": "M",
}


def _map_interval(interval: str) -> str:
    return _INTERVAL_MAP.get(interval, interval)


class BybitRestError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


class BybitFuturesREST:
    """Aiohttp-backed REST client with retries and a shared connector pool.

    Method signatures match the previous Binance client so callers don't
    need to change — responses are normalized to Binance-style shapes
    where the engine reads them.
    """

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        *,
        category: str = "linear",
        timeout: float = 10.0,
        max_retries: int = 4,
        user_agent: str = "dump_bot/0.1",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.category = category
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self._session: aiohttp.ClientSession | None = None
        self._user_agent = user_agent
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
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt > self.max_retries:
                    raise BybitRestError(0, f"network error: {exc}") from exc
                wait = min(15.0, 0.5 * (2 ** attempt))
                log.warning("bybit_net_error",
                            extra={"path": path, "attempt": attempt, "wait": wait,
                                   "err": repr(exc)})
                await asyncio.sleep(wait)
                continue

            ret_code = body.get("retCode") if isinstance(body, dict) else None
            if ret_code not in (0, None):
                msg = body.get("retMsg", "")
                # 10006 / 10018 etc. are rate limit related; retry a few times.
                if ret_code in (10006, 10018) and attempt <= self.max_retries:
                    await asyncio.sleep(min(15.0, 0.5 * (2 ** attempt)))
                    continue
                raise BybitRestError(200, f"retCode={ret_code} {msg}")
            return body

    # ---------- Public endpoints (normalized output) ----------

    async def exchange_info(self) -> dict[str, Any]:
        """Return ``{"symbols": [...]}`` shaped like Binance exchangeInfo.

        Bybit paginates instruments-info via ``nextPageCursor``; we walk
        all pages so the universe sees every active perpetual.
        """
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"category": self.category, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            body = await self._request("/v5/market/instruments-info", params=params)
            result = body.get("result") or {}
            for s in result.get("list") or []:
                # Bybit ``contractType`` is e.g. "LinearPerpetual"; the engine
                # filters on "PERPETUAL", so collapse here.
                ctype_raw = (s.get("contractType") or "").upper()
                contract_type = "PERPETUAL" if "PERPETUAL" in ctype_raw else ctype_raw
                status_raw = (s.get("status") or "").upper()
                status = "TRADING" if status_raw == "TRADING" else status_raw
                out.append({
                    "symbol": (s.get("symbol") or "").upper(),
                    "contractType": contract_type,
                    "status": status,
                    "baseAsset": (s.get("baseCoin") or "").upper(),
                    "quoteAsset": (s.get("quoteCoin") or "").upper(),
                })
            cursor = (result.get("nextPageCursor") or "").strip()
            if not cursor:
                break
        return {"symbols": out}

    async def ticker_24h(self) -> list[dict[str, Any]]:
        """All-symbol 24h tickers, normalized to Binance keys."""
        body = await self._request(
            "/v5/market/tickers", params={"category": self.category}
        )
        result = body.get("result") or {}
        out: list[dict[str, Any]] = []
        for t in result.get("list") or []:
            try:
                pct = float(t.get("price24hPcnt") or 0.0) * 100.0
            except (TypeError, ValueError):
                pct = 0.0
            out.append({
                "symbol": (t.get("symbol") or "").upper(),
                # Bybit ``turnover24h`` is quote-asset notional (= Binance quoteVolume).
                "quoteVolume": t.get("turnover24h") or "0",
                "volume": t.get("volume24h") or "0",
                "lastPrice": t.get("lastPrice") or "0",
                "priceChangePercent": str(pct),
                "highPrice": t.get("highPrice24h") or "0",
                "lowPrice": t.get("lowPrice24h") or "0",
                "openPrice": t.get("prevPrice24h") or "0",
            })
        return out

    async def book_ticker(self) -> list[dict[str, Any]]:
        """All-symbol best bid/ask. Bybit exposes bid1/ask1 inside the
        tickers endpoint, so re-use that payload."""
        body = await self._request(
            "/v5/market/tickers", params={"category": self.category}
        )
        result = body.get("result") or {}
        out: list[dict[str, Any]] = []
        for t in result.get("list") or []:
            out.append({
                "symbol": (t.get("symbol") or "").upper(),
                "bidPrice": t.get("bid1Price") or "0",
                "bidQty": t.get("bid1Size") or "0",
                "askPrice": t.get("ask1Price") or "0",
                "askQty": t.get("ask1Size") or "0",
            })
        return out

    async def open_interest(self, symbol: str) -> dict[str, Any]:
        """Latest open interest for a single symbol.

        Bybit only exposes OI history (5min/15min/30min/1h/4h/1d). We pull
        the most recent 5-minute datapoint and normalize to the shape the
        engine expects: ``{"symbol": ..., "openInterest": "...", "time": ms}``.
        """
        body = await self._request(
            "/v5/market/open-interest",
            params={
                "category": self.category,
                "symbol": symbol,
                "intervalTime": "5min",
                "limit": 1,
            },
        )
        result = body.get("result") or {}
        rows = result.get("list") or []
        if not rows:
            return {"symbol": symbol, "openInterest": "0", "time": 0}
        row = rows[0]
        return {
            "symbol": symbol,
            "openInterest": row.get("openInterest") or "0",
            "time": int(row.get("timestamp") or 0),
        }

    async def funding_rate(
        self, symbol: str | None = None, limit: int = 1
    ) -> list[dict[str, Any]]:
        """Funding history. With no symbol this is meaningless on Bybit
        (the endpoint requires a symbol), so we just return an empty list
        — the engine only calls this per-symbol."""
        if symbol is None:
            return []
        body = await self._request(
            "/v5/market/funding/history",
            params={
                "category": self.category,
                "symbol": symbol,
                "limit": max(1, min(200, limit)),
            },
        )
        result = body.get("result") or {}
        out: list[dict[str, Any]] = []
        for r in result.get("list") or []:
            out.append({
                "symbol": (r.get("symbol") or symbol).upper(),
                "fundingRate": r.get("fundingRate") or "0",
                "fundingTime": int(r.get("fundingRateTimestamp") or 0),
            })
        return out

    async def premium_index(self, symbol: str | None = None) -> Any:
        """Best-effort premium index from the tickers endpoint."""
        if symbol is None:
            tickers = await self.ticker_24h()
            return tickers
        body = await self._request(
            "/v5/market/tickers",
            params={"category": self.category, "symbol": symbol},
        )
        result = body.get("result") or {}
        rows = result.get("list") or []
        if not rows:
            return {}
        t = rows[0]
        try:
            funding = float(t.get("fundingRate") or 0.0)
        except (TypeError, ValueError):
            funding = 0.0
        return {
            "symbol": symbol,
            "markPrice": t.get("markPrice") or "0",
            "indexPrice": t.get("indexPrice") or "0",
            "lastFundingRate": str(funding),
            "nextFundingTime": int(t.get("nextFundingTime") or 0),
        }

    async def klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[list[Any]]:
        """Klines in ascending chronological order, returned as
        Binance-style arrays:
            [open_ms, open, high, low, close, volume, close_ms,
             quote_volume, trades, taker_buy_vol, taker_buy_quote_vol, ignore]

        Bybit doesn't expose per-candle trade count or taker-buy splits in
        public REST, so those fields are filled with zeros — the strategy
        already tolerates that (it reads them as proxies, not invariants).
        """
        params: dict[str, Any] = {
            "category": self.category,
            "symbol": symbol,
            "interval": _map_interval(interval),
            "limit": max(1, min(1000, limit)),
        }
        if start_time is not None:
            params["start"] = int(start_time)
        if end_time is not None:
            params["end"] = int(end_time)
        body = await self._request("/v5/market/kline", params=params)
        result = body.get("result") or {}
        rows = result.get("list") or []

        # Estimate candle duration from the requested interval so we can
        # produce a sensible close_ms (Bybit only returns the open ts).
        duration_ms = _interval_duration_ms(interval)

        out: list[list[Any]] = []
        # Bybit returns newest-first; reverse to match Binance ordering.
        for r in reversed(rows):
            try:
                open_ms = int(r[0])
                o = float(r[1])
                h = float(r[2])
                l = float(r[3])
                c = float(r[4])
                vol = float(r[5])
                turnover = float(r[6]) if len(r) > 6 else 0.0
            except (TypeError, ValueError, IndexError):
                continue
            close_ms = open_ms + duration_ms - 1 if duration_ms else open_ms
            out.append([open_ms, o, h, l, c, vol, close_ms, turnover, 0, 0.0, 0.0, 0])
        return out


def _interval_duration_ms(interval: str) -> int:
    """Best-effort interval -> milliseconds. Used to synthesize close_ms."""
    table = {
        "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
        "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
        "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000,
    }
    return table.get(interval, 60_000)


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
