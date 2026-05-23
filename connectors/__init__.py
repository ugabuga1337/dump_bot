"""Connectors package."""

from .binance_rest import (
    BinanceFuturesREST,
    BinanceRestError,
    BybitFuturesREST,
    BybitRestError,
)
from .binance_ws import BinanceWSClient, BybitWSClient, StreamManager

__all__ = [
    "BybitFuturesREST",
    "BybitRestError",
    "BybitWSClient",
    # Legacy aliases — same classes, kept so older imports don't break.
    "BinanceFuturesREST",
    "BinanceRestError",
    "BinanceWSClient",
    "StreamManager",
]
