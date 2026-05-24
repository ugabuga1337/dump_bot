"""Connectors package."""

from .bybit_rest import BybitFuturesREST, BybitRestError
from .bybit_ws import BybitWSClient, StreamManager

# Backwards-compatible aliases — keep older references working without a
# rename sweep across the codebase. The transport is Bybit; the names just
# stick to what the rest of the engine already imports.
BinanceFuturesREST = BybitFuturesREST
BinanceRestError = BybitRestError
BinanceWSClient = BybitWSClient

__all__ = [
    "BybitFuturesREST",
    "BybitRestError",
    "BybitWSClient",
    "StreamManager",
    # legacy aliases
    "BinanceFuturesREST",
    "BinanceRestError",
    "BinanceWSClient",
]
