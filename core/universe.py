"""Universe discovery and filtering.

Pulls Binance Futures USDT-M perpetual contracts via REST, filters by
volume/price/blacklist, and exposes the current trading universe as a list
of symbol identifiers.
"""

from __future__ import annotations

import logging
from typing import Any

from config import UniverseConfig
from connectors import BybitFuturesREST

log = logging.getLogger("universe")


_LEVERAGED_TOKENS = ("UP", "DOWN", "BULL", "BEAR")


def _looks_leveraged(symbol: str, base: str) -> bool:
    s = symbol.upper()
    for tag in _LEVERAGED_TOKENS:
        if s.endswith(f"{tag}USDT"):
            return True
    return False


class Universe:
    def __init__(self, cfg: UniverseConfig, rest: BybitFuturesREST) -> None:
        self._cfg = cfg
        self._rest = rest
        self._symbols: list[str] = []
        self._symbol_meta: dict[str, dict[str, Any]] = {}
        # Live overrides — populated from bot_settings on each scan cycle.
        self._max_symbols_override: int | None = None
        self._min_change_pct_override: float | None = None

    def set_overrides(
        self,
        *,
        max_symbols: int | None = None,
        min_change_pct: float | None = None,
    ) -> None:
        self._max_symbols_override = max_symbols
        self._min_change_pct_override = min_change_pct

    @property
    def symbols(self) -> list[str]:
        return list(self._symbols)

    def meta(self, symbol: str) -> dict[str, Any]:
        return self._symbol_meta.get(symbol, {})

    async def refresh(self) -> list[str]:
        info = await self._rest.exchange_info()
        ticker = await self._rest.ticker_24h()
        ticker_map = {t["symbol"]: t for t in ticker}

        tradable: list[dict[str, Any]] = []
        whitelist = set(self._cfg.whitelist)
        blacklist = set(self._cfg.blacklist)

        for s in info.get("symbols", []):
            symbol = s.get("symbol", "").upper()
            contract_type = s.get("contractType", "")
            status = s.get("status", "")
            quote = s.get("quoteAsset", "").upper()
            base = s.get("baseAsset", "").upper()

            if status != "TRADING":
                continue
            if contract_type != "PERPETUAL":
                continue
            if quote != self._cfg.quote_asset:
                continue
            if symbol in blacklist:
                continue
            if whitelist and symbol not in whitelist:
                continue
            if self._cfg.exclude_leveraged and _looks_leveraged(symbol, base):
                continue

            t = ticker_map.get(symbol)
            if not t:
                continue
            try:
                quote_volume = float(t.get("quoteVolume", 0.0))
                last_price = float(t.get("lastPrice", 0.0))
                price_change_pct = float(t.get("priceChangePercent", 0.0))
            except (TypeError, ValueError):
                continue

            if quote_volume < self._cfg.min_quote_volume_24h:
                continue
            if last_price < self._cfg.min_price:
                continue
            if (
                self._min_change_pct_override is not None
                and price_change_pct < self._min_change_pct_override
            ):
                continue

            tradable.append(
                {
                    "symbol": symbol,
                    "base_asset": base,
                    "quote_asset": quote,
                    "quote_volume_24h": quote_volume,
                    "last_price": last_price,
                    "price_change_pct": price_change_pct,
                }
            )

        # When a gainer override is in effect, prefer top-N by 24h change.
        if self._min_change_pct_override is not None:
            tradable.sort(key=lambda r: r["price_change_pct"], reverse=True)
        else:
            tradable.sort(key=lambda r: r["quote_volume_24h"], reverse=True)
        cap = self._max_symbols_override or self._cfg.max_symbols
        if len(tradable) > cap:
            tradable = tradable[:cap]

        self._symbols = [r["symbol"] for r in tradable]
        self._symbol_meta = {r["symbol"]: r for r in tradable}
        log.info("universe_refreshed", extra={"count": len(self._symbols)})
        return self._symbols

    def export_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "symbol": r["symbol"],
                "quote_asset": r["quote_asset"],
                "last_price": r["last_price"],
                "quote_volume_24h": r["quote_volume_24h"],
                "price_change_pct": r["price_change_pct"],
                "active": 1,
            }
            for r in self._symbol_meta.values()
        ]
