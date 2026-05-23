"""Config package: top-level Settings access."""

from .settings import (
    BinanceConfig,
    DashboardConfig,
    ExchangeConfig,
    OutcomeConfig,
    PumpConfig,
    RegimeConfig,
    Settings,
    SignalConfig,
    TelegramConfig,
    UniverseConfig,
    get_settings,
    reset_settings_for_tests,
)

__all__ = [
    "ExchangeConfig",
    "BinanceConfig",  # legacy alias
    "DashboardConfig",
    "OutcomeConfig",
    "PumpConfig",
    "RegimeConfig",
    "Settings",
    "SignalConfig",
    "TelegramConfig",
    "UniverseConfig",
    "get_settings",
    "reset_settings_for_tests",
]
