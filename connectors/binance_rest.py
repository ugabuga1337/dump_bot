"""Binance Futures (USDT-M) public REST client.

Read-only — only public endpoints are used. No API key, no signing, no risk
of accidental order placement.

Returns are plain dicts/lists; the consumer normalizes into models.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

log = logging.getLogger("binance.rest")


class BinanceRestError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


class BinanceFuturesREST:
    """Aiohttp-backed REST client with retries, rate limit handling, and
    a shared connector pool kept small to fit a 1GB VPS."""

    def __init__(
        self,
        base_url: str = "https://fapi.binance.com",
        *,
        timeout: float = 10.0,
        max_retries: int = 4,
        user_agent: str = "dump_bot/0.1",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self._session: aiohttp.ClientSession | None = None
        self._user_agent = user_agent
        self._closed = False

    async def __aenter__(self) -> BinanceFuturesREST:
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
                            "binance_rate_limited",
                            extra={"path": path, "status": resp.status, "retry": retry},
                        )
                        await asyncio.sleep(min(30.0, retry))
                        continue
                    if 500 <= resp.status < 600:
                        text = await resp.text()
                        log.warning("binance_5xx",
                                    extra={"path": path, "status": resp.status,
                                           "body": text[:200]})
                        if attempt > self.max_retries:
                            raise BinanceRestError(resp.status, text)
                        await asyncio.sleep(min(15.0, 0.5 * (2 ** attempt)))
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        raise BinanceRestError(resp.status, text)
                    return await resp.json(loads=_loads)
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt > self.max_retries:
                    raise BinanceRestError(0, f"network error: {exc}") from exc
                wait = min(15.0, 0.5 * (2 ** attempt))
                log.warning("binance_net_error",
                            extra={"path": path, "attempt": attempt, "wait": wait,
                                   "err": repr(exc)})
                await asyncio.sleep(wait)

    # ---------- Public endpoints ----------

    async def exchange_info(self) -> dict[str, Any]:
        return await self._request("/fapi/v1/exchangeInfo")

    async def ticker_24h(self) -> list[dict[str, Any]]:
        return await self._request("/fapi/v1/ticker/24hr")

    async def book_ticker(self) -> list[dict[str, Any]]:
        return await self._request("/fapi/v1/ticker/bookTicker")

    async def open_interest(self, symbol: str) -> dict[str, Any]:
        return await self._request("/fapi/v1/openInterest", params={"symbol": symbol})

    async def funding_rate(self, symbol: str | None = None, limit: int = 1) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        return await self._request("/fapi/v1/fundingRate", params=params)

    async def premium_index(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol} if symbol else None
        return await self._request("/fapi/v1/premiumIndex", params=params)

    async def klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return await self._request("/fapi/v1/klines", params=params)


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
