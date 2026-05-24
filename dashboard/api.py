"""Dashboard API routes.

All endpoints return JSON or HTML fragments (HTMX) — no heavy frontend.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from storage import Repository

from .templating import templates

api_router = APIRouter()
log = logging.getLogger("dashboard")


def get_repo(request: Request) -> Repository:
    return request.app.state.repo


def _parse_json(value: Any, default: Any) -> Any:
    if not value:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _enrich_signal(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row["reasons"] = _parse_json(row.pop("reasons_json", None), [])
    row["setup_tags"] = _parse_json(row.pop("setup_tags_json", None), [])
    row["metadata"] = _parse_json(row.pop("metadata_json", None), {})
    return row


def _opt_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _opt_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    return v or None


async def _build_status(request: Request, repo: Repository) -> dict[str, Any]:
    snap = await repo.latest_market_snapshot()
    last_sig = await repo.list_signals(limit=1)
    metadata = _parse_json(snap.get("metadata_json"), {}) if snap else {}
    return {
        "uptime_sec": int(time.time() - request.app.state.started_at),
        "regime": snap.get("btc_regime") if snap else None,
        "btc_price": snap.get("btc_price") if snap else None,
        "btc_trend_pct": snap.get("btc_trend_pct") if snap else None,
        "btc_atr_pct": snap.get("btc_atr_pct") if snap else None,
        "overheated": snap.get("overheated_count") if snap else None,
        "ws": metadata.get("ws_shards") if metadata else [],
        "telegram": metadata.get("telegram") if metadata else {},
        "signals_emitted_total": metadata.get("signals_emitted") if metadata else None,
        "pumps_detected_total": metadata.get("pumps_detected") if metadata else None,
        "messages_seen": metadata.get("messages_seen") if metadata else None,
        "last_signal": _enrich_signal(last_sig[0]) if last_sig else None,
        "snapshot_age_sec": int(time.time() - (snap.get("ts_ms", 0) / 1000)) if snap and snap.get("ts_ms") else None,
        "active_cooldowns": [],
    }


async def _fetch_signals(
    repo: Repository,
    *,
    limit: int,
    offset: int = 0,
    symbol: Any = None,
    min_confidence: Any = None,
    setup: Any = None,
    since_hours: Any = None,
) -> list[dict[str, Any]]:
    since_hours_v = _opt_int(since_hours)
    since_ms = None
    if since_hours_v is not None and since_hours_v > 0:
        since_ms = int((time.time() - since_hours_v * 3600) * 1000)
    rows = await repo.list_signals(
        limit=limit,
        offset=offset,
        symbol=_opt_str(symbol),
        min_confidence=_opt_float(min_confidence),
        setup_tag=_opt_str(setup),
        since_ms=since_ms,
    )
    return [_enrich_signal(r) for r in rows]


# ---------- JSON endpoints ----------


@api_router.get("/status")
async def api_status(request: Request, repo: Repository = Depends(get_repo)):
    return await _build_status(request, repo)


@api_router.get("/signals")
async def api_signals(
    request: Request,
    repo: Repository = Depends(get_repo),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    symbol: str | None = None,
    min_confidence: str | None = None,
    setup: str | None = None,
    since_hours: str | None = None,
):
    return await _fetch_signals(
        repo, limit=limit, offset=offset, symbol=symbol,
        min_confidence=min_confidence, setup=setup, since_hours=since_hours,
    )


@api_router.get("/analytics/summary")
async def api_summary(
    request: Request,
    repo: Repository = Depends(get_repo),
    days: int = Query(7, ge=1, le=90),
):
    since_ms = int((time.time() - days * 24 * 3600) * 1000)
    agg = await repo.aggregate_signal_stats(since_ms=since_ms)
    total = agg.get("total") or 0
    wins = agg.get("wins") or 0
    losses = agg.get("losses") or 0
    winrate = (wins / (wins + losses) * 100.0) if (wins + losses) else 0.0
    return {
        "window_days": days,
        "total": total,
        "wins": wins,
        "losses": losses,
        "winrate_pct": round(winrate, 2),
        "avg_mfe": round(agg.get("avg_mfe") or 0.0, 3),
        "avg_mae": round(agg.get("avg_mae") or 0.0, 3),
        "avg_confidence": round(agg.get("avg_conf") or 0.0, 2),
    }


async def _fetch_per_symbol(repo: Repository) -> list[dict[str, Any]]:
    rows = await repo.per_symbol_stats(limit=100)
    out = []
    for r in rows:
        total = r.get("total") or 0
        wins = r.get("wins") or 0
        out.append({
            "symbol": r.get("symbol"),
            "total": total,
            "wins": wins,
            "winrate_pct": round((wins / total * 100.0) if total else 0.0, 2),
            "avg_mfe": round(r.get("avg_mfe") or 0.0, 3),
            "avg_mae": round(r.get("avg_mae") or 0.0, 3),
            "avg_confidence": round(r.get("avg_conf") or 0.0, 2),
        })
    return out


async def _fetch_per_setup(repo: Repository) -> list[dict[str, Any]]:
    rows = await repo.per_setup_stats()
    out = []
    for r in rows:
        total = r.get("total") or 0
        wins = r.get("wins") or 0
        out.append({
            "tag": r.get("tag"),
            "total": total,
            "wins": wins,
            "winrate_pct": round((wins / total * 100.0) if total else 0.0, 2),
            "avg_mfe": round(r.get("avg_mfe") or 0.0, 3),
            "avg_mae": round(r.get("avg_mae") or 0.0, 3),
            "avg_confidence": round(r.get("avg_conf") or 0.0, 2),
        })
    return out


@api_router.get("/analytics/per_symbol")
async def api_per_symbol(repo: Repository = Depends(get_repo)):
    return await _fetch_per_symbol(repo)


@api_router.get("/analytics/per_setup")
async def api_per_setup(repo: Repository = Depends(get_repo)):
    return await _fetch_per_setup(repo)


# ---------- HTMX fragments ----------


@api_router.get("/fragments/status")
async def fragment_status(request: Request,
                          repo: Repository = Depends(get_repo)):
    try:
        status = await _build_status(request, repo)
    except Exception:  # noqa: BLE001
        log.exception("fragment_status_failed")
        return templates.TemplateResponse(
            request, "fragments/status.html",
            {"status": None, "load_error": True},
            status_code=200,
        )
    return templates.TemplateResponse(
        request, "fragments/status.html", {"status": status},
    )


@api_router.get("/fragments/signals")
async def fragment_signals(request: Request,
                           repo: Repository = Depends(get_repo),
                           limit: int = Query(25, ge=1, le=200),
                           symbol: str | None = None,
                           min_confidence: str | None = None,
                           setup: str | None = None,
                           since_hours: str | None = None):
    try:
        rows = await _fetch_signals(
            repo, limit=limit, symbol=symbol,
            min_confidence=min_confidence, setup=setup,
            since_hours=since_hours,
        )
    except Exception:  # noqa: BLE001 — render the error instead of 500'ing the fragment
        log.exception("fragment_signals_failed")
        return templates.TemplateResponse(
            request, "fragments/signals_table.html", {"signals": None},
            status_code=200,
        )
    return templates.TemplateResponse(
        request, "fragments/signals_table.html", {"signals": rows},
    )


@api_router.get("/fragments/coins")
async def fragment_coins(request: Request,
                         repo: Repository = Depends(get_repo)):
    try:
        rows = await _fetch_per_symbol(repo)
    except Exception:  # noqa: BLE001
        log.exception("fragment_coins_failed")
        rows = None  # type: ignore[assignment]
    return templates.TemplateResponse(
        request, "fragments/coins_table.html", {"coins": rows},
    )


@api_router.get("/fragments/patterns")
async def fragment_patterns(request: Request,
                            repo: Repository = Depends(get_repo)):
    try:
        rows = await _fetch_per_setup(repo)
    except Exception:  # noqa: BLE001
        log.exception("fragment_patterns_failed")
        rows = None  # type: ignore[assignment]
    return templates.TemplateResponse(
        request, "fragments/patterns_table.html", {"patterns": rows},
    )
