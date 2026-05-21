"""Shared Jinja2 templates instance."""

from pathlib import Path

from fastapi.templating import Jinja2Templates

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _format_pct(value, signed: bool = True) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if signed and v > 0 else ""
    return f"{sign}{v:.2f}%"


def _format_price(value) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def _format_ts(value) -> str:
    import time as _time

    try:
        v = float(value) / 1000.0
    except (TypeError, ValueError):
        return ""
    return _time.strftime("%Y-%m-%d %H:%M:%S", _time.gmtime(v))


templates.env.filters["pct"] = _format_pct
templates.env.filters["price"] = _format_price
templates.env.filters["ts"] = _format_ts
