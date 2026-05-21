"""Runtime configuration.

Loads from environment + optional ``config/default.yaml``.

Designed so every subsystem can pull a tiny typed dataclass instead of reading
``os.environ`` ad hoc — makes tests trivial and keeps secrets centralized.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return v


def _env_float(name: str, default: float) -> float:
    v = _env(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: list[str] | None = None) -> list[str]:
    v = _env(name)
    if v is None:
        return list(default or [])
    return [x.strip().upper() for x in v.split(",") if x.strip()]


@dataclass(slots=True, frozen=True)
class BinanceConfig:
    rest_url: str = "https://fapi.binance.com"
    ws_url: str = "wss://fstream.binance.com"
    ws_combined: str = "wss://fstream.binance.com/stream"
    # Read-only operation; we don't use private endpoints anywhere.


@dataclass(slots=True, frozen=True)
class UniverseConfig:
    quote_asset: str = "USDT"
    min_quote_volume_24h: float = 30_000_000.0
    min_price: float = 0.0005
    max_symbols: int = 120
    refresh_minutes: int = 15
    whitelist: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    exclude_leveraged: bool = True


@dataclass(slots=True, frozen=True)
class PumpConfig:
    min_5m_pct: float = 4.0
    min_15m_pct: float = 7.0
    min_volume_ratio: float = 3.0
    min_oi_pct: float = 2.5
    relative_btc: float = 1.8
    score_threshold: float = 55.0


@dataclass(slots=True, frozen=True)
class SignalConfig:
    exhaustion_threshold: float = 65.0
    confidence_low: float = 55.0
    confidence_med: float = 70.0
    confidence_high: float = 82.0
    watch_ttl_sec: int = 900
    cooldown_sec: int = 1800


@dataclass(slots=True, frozen=True)
class TelegramConfig:
    bot_token: str = ""
    chat_id: str = ""
    enabled: bool = True

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.bot_token and self.chat_id)


@dataclass(slots=True, frozen=True)
class RegimeConfig:
    btc_symbol: str = "BTCUSDT"
    btc_trend_lookback: int = 240
    btc_atr_period: int = 14


@dataclass(slots=True, frozen=True)
class DashboardConfig:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080
    user: str = "admin"
    password: str = "change-me"
    secret: str = "please-change-this-long-random-string"
    ip_whitelist: list[str] = field(default_factory=list)
    behind_proxy: bool = False


@dataclass(slots=True, frozen=True)
class OutcomeConfig:
    track_seconds: int = 3600
    invalidation_pct: float = 2.0


@dataclass(slots=True, frozen=True)
class Settings:
    log_level: str
    log_json: bool
    data_dir: Path
    db_path: Path
    timezone: str
    binance: BinanceConfig
    universe: UniverseConfig
    pump: PumpConfig
    signal: SignalConfig
    telegram: TelegramConfig
    regime: RegimeConfig
    dashboard: DashboardConfig
    outcome: OutcomeConfig
    raw_yaml: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, env_file: str | os.PathLike | None = None) -> Settings:
        if env_file is None:
            # Locate .env at repo root if it exists; harmless if absent.
            root = Path(__file__).resolve().parent.parent
            candidate = root / ".env"
            if candidate.exists():
                env_file = candidate
        if env_file:
            load_dotenv(env_file, override=False)

        # Optional YAML overlay
        yaml_data: dict[str, Any] = {}
        yaml_path = Path(__file__).resolve().parent / "default.yaml"
        if yaml_path.exists():
            try:
                yaml_data = yaml.safe_load(yaml_path.read_text()) or {}
            except Exception:  # noqa: BLE001 — config errors must not crash startup
                yaml_data = {}

        data_dir = Path(_env("DATA_DIR", "/data") or "/data")
        db_path = Path(_env("DB_PATH", str(data_dir / "dump_bot.sqlite3")) or "")

        return cls(
            log_level=_env("LOG_LEVEL", "INFO") or "INFO",
            log_json=_env_bool("LOG_JSON", False),
            data_dir=data_dir,
            db_path=db_path,
            timezone=_env("TIMEZONE", "UTC") or "UTC",
            binance=BinanceConfig(
                rest_url=_env("BINANCE_FUTURES_REST", "https://fapi.binance.com") or "",
                ws_url=_env("BINANCE_FUTURES_WS", "wss://fstream.binance.com") or "",
                ws_combined=_env("BINANCE_FUTURES_WS_COMBINED",
                                 "wss://fstream.binance.com/stream") or "",
            ),
            universe=UniverseConfig(
                quote_asset=(_env("QUOTE_ASSET", "USDT") or "USDT").upper(),
                min_quote_volume_24h=_env_float("MIN_QUOTE_VOLUME_24H", 30_000_000.0),
                min_price=_env_float("MIN_PRICE", 0.0005),
                max_symbols=_env_int("MAX_SYMBOLS", 120),
                refresh_minutes=_env_int("UNIVERSE_REFRESH_MIN", 15),
                whitelist=_env_list("WHITELIST", []),
                blacklist=_env_list("BLACKLIST", ["BTCUSDT", "ETHUSDT"]),
                exclude_leveraged=_env_bool("EXCLUDE_LEVERAGED", True),
            ),
            pump=PumpConfig(
                min_5m_pct=_env_float("PUMP_MIN_5M_PCT", 4.0),
                min_15m_pct=_env_float("PUMP_MIN_15M_PCT", 7.0),
                min_volume_ratio=_env_float("PUMP_MIN_VOL_RATIO", 3.0),
                min_oi_pct=_env_float("PUMP_MIN_OI_PCT", 2.5),
                relative_btc=_env_float("PUMP_RELATIVE_BTC", 1.8),
                score_threshold=_env_float("PUMP_SCORE_THRESHOLD", 55.0),
            ),
            signal=SignalConfig(
                exhaustion_threshold=_env_float("EXHAUSTION_SCORE_THRESHOLD", 65.0),
                confidence_low=_env_float("CONFIDENCE_LOW", 55.0),
                confidence_med=_env_float("CONFIDENCE_MED", 70.0),
                confidence_high=_env_float("CONFIDENCE_HIGH", 82.0),
                watch_ttl_sec=_env_int("WATCH_TTL_SEC", 900),
                cooldown_sec=_env_int("COOLDOWN_SEC", 1800),
            ),
            telegram=TelegramConfig(
                bot_token=_env("TELEGRAM_BOT_TOKEN", "") or "",
                chat_id=_env("TELEGRAM_CHAT_ID", "") or "",
                enabled=_env_bool("TELEGRAM_ENABLED", True),
            ),
            regime=RegimeConfig(
                btc_symbol=(_env("BTC_REGIME_SYMBOL", "BTCUSDT") or "BTCUSDT").upper(),
                btc_trend_lookback=_env_int("BTC_TREND_LOOKBACK", 240),
                btc_atr_period=_env_int("BTC_ATR_PERIOD", 14),
            ),
            dashboard=DashboardConfig(
                enabled=_env_bool("DASHBOARD_ENABLED", True),
                host=_env("DASHBOARD_HOST", "0.0.0.0") or "0.0.0.0",
                port=_env_int("DASHBOARD_PORT", 8080),
                user=_env("DASHBOARD_USER", "admin") or "admin",
                password=_env("DASHBOARD_PASSWORD", "change-me") or "change-me",
                secret=_env("DASHBOARD_SECRET", "please-change-this-long-random-string")
                or "please-change-this-long-random-string",
                ip_whitelist=_env_list("DASHBOARD_IP_WHITELIST", []),
                behind_proxy=_env_bool("DASHBOARD_BEHIND_PROXY", False),
            ),
            outcome=OutcomeConfig(
                track_seconds=_env_int("OUTCOME_TRACK_SEC", 3600),
                invalidation_pct=_env_float("OUTCOME_INVALIDATION_PCT", 2.0),
            ),
            raw_yaml=yaml_data,
        )

    def ensure_paths(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the singleton settings, loading on first call."""
    global _settings
    if _settings is None:
        _settings = Settings.load()
        _settings.ensure_paths()
    return _settings


def reset_settings_for_tests() -> None:
    """Allow tests to force re-load from the environment."""
    global _settings
    _settings = None
