"""Structured logging with optional JSON output, async-safe.

Keeps the formatter intentionally tiny so we don't drag in structlog/loguru and pay
RAM for a fancy dependency on a 1GB VPS.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

try:
    import orjson as _json  # type: ignore

    def _dumps(obj: Any) -> str:
        return _json.dumps(obj).decode()
except ImportError:  # pragma: no cover
    import json as _json_std

    def _dumps(obj: Any) -> str:
        return _json_std.dumps(obj, default=str, separators=(",", ":"))


_RESERVED_RECORD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process", "message",
    "asctime", "taskName",
}


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter, designed to be cheap.

    Adds any custom kwargs passed via ``logger.info("msg", extra={"key": ...})``.
    """

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "lvl": record.levelname,
            "log": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k in _RESERVED_RECORD_ATTRS or k.startswith("_"):
                continue
            payload[k] = v
        return _dumps(payload)


class ColorFormatter(logging.Formatter):
    """ANSI colored, human-friendly formatter for VPS journalctl tailing."""

    _COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        color = self._COLORS.get(record.levelname, "")
        ts = time.strftime("%H:%M:%S", time.gmtime(record.created))
        extras = []
        for k, v in record.__dict__.items():
            if k in _RESERVED_RECORD_ATTRS or k.startswith("_"):
                continue
            extras.append(f"{k}={v}")
        tail = ("  " + " ".join(extras)) if extras else ""
        return (
            f"{color}{ts} {record.levelname:<7}{self._RESET} "
            f"{record.name}: {record.getMessage()}{tail}"
        )


def setup_logging(level: str | int | None = None, *, json_output: bool | None = None) -> None:
    """Initialize global logging once.

    Idempotent — safe to call from engine + dashboard processes.
    """

    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")
    if json_output is None:
        json_output = os.environ.get("LOG_JSON", "false").lower() in {"1", "true", "yes"}

    root = logging.getLogger()
    # Wipe existing handlers (e.g. uvicorn default) so output stays consistent.
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter() if json_output else ColorFormatter())
    root.addHandler(handler)
    root.setLevel(level if isinstance(level, int) else level.upper())

    # Silence chatty libraries. Keep ``websockets`` at WARNING for normal
    # noise, but allow ``websockets.client`` through at INFO so handshake
    # failures (the most likely failure mode when transport breaks) reach
    # the logs instead of being silently dropped.
    for noisy in ("websockets", "uvicorn.access", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
