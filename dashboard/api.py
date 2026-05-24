"""Dashboard API routes.

All endpoints return JSON or HTML fragments (HTMX) — no heavy frontend.
"""

from __future__ import annotations

import json
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from storage import Repository

from .templating import templates

api_router = APIRouter()


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


# ---------- JSON endpoints ----------


@api_router.get("/status")
async def api_status(request: Request, repo: Repository = Depends(get_repo)):
    snap = await repo.latest_market_snapshot()
    last_sig = await repo.list_signals(limit=1)
    cooldowns = []
    metadata = _parse_json(snap.get("metadata_json"), {}) if snap else {}
    ws_list = metadata.get("ws_shards") if metadata else []
    ws_list = ws_list or []
    ws_total = len(ws_list)
    ws_connected = sum(1 for s in ws_list if s.get("connected"))
    ws_down = ws_total - ws_connected
    ws_ok = ws_total > 0 and ws_down == 0
    return {
        "uptime_sec": int(time.time() - request.app.state.started_at),
        "regime": snap.get("btc_regime") if snap else None,
        "btc_price": snap.get("btc_price") if snap else None,
        "btc_trend_pct": snap.get("btc_trend_pct") if snap else None,
        "btc_atr_pct": snap.get("btc_atr_pct") if snap else None,
        "overheated": snap.get("overheated_count") if snap else None,
        "ws": ws_list,
        "ws_total": ws_total,
        "ws_connected": ws_connected,
        "ws_down": ws_down,
        "ws_ok": ws_ok,
        "telegram": metadata.get("telegram") if metadata else {},
        "signals_emitted_total": metadata.get("signals_emitted") if metadata else None,
        "pumps_detected_total": metadata.get("pumps_detected") if metadata else None,
        "messages_seen": metadata.get("messages_seen") if metadata else None,
        "monitored": metadata.get("monitored") if metadata else None,
        "watch_count": metadata.get("watch_count") if metadata else None,
        "cooldown_count": metadata.get("cooldown_count") if metadata else None,
        "last_signal": _enrich_signal(last_sig[0]) if last_sig else None,
        "snapshot_age_sec": int(time.time() - (snap.get("ts_ms", 0) / 1000)) if snap and snap.get("ts_ms") else None,
        "active_cooldowns": cooldowns,
    }


@api_router.get("/signals")
async def api_signals(
    request: Request,
    repo: Repository = Depends(get_repo),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    symbol: str | None = None,
    min_confidence: float | None = None,
    setup: str | None = None,
    since_hours: int | None = None,
):
    since_ms = None
    if since_hours is not None and since_hours > 0:
        since_ms = int((time.time() - since_hours * 3600) * 1000)
    rows = await repo.list_signals(
        limit=limit,
        offset=offset,
        symbol=symbol,
        min_confidence=min_confidence,
        setup_tag=setup,
        since_ms=since_ms,
    )
    return [_enrich_signal(r) for r in rows]


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


@api_router.get("/analytics/per_symbol")
async def api_per_symbol(repo: Repository = Depends(get_repo)):
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


@api_router.get("/analytics/per_setup")
async def api_per_setup(repo: Repository = Depends(get_repo)):
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


@api_router.get("/analytics/timeline")
async def api_timeline(repo: Repository = Depends(get_repo),
                      days: int = Query(30, ge=1, le=120)):
    return await repo.signals_per_day(days=days)


# ---------- HTMX fragments ----------


@api_router.get("/fragments/status")
async def fragment_status(request: Request,
                          repo: Repository = Depends(get_repo)):
    status = await api_status(request, repo)
    return templates.TemplateResponse(
        request, "fragments/status.html", {"status": status},
    )


@api_router.get("/fragments/signals")
async def fragment_signals(request: Request,
                           repo: Repository = Depends(get_repo),
                           limit: int = Query(25, ge=1, le=200),
                           symbol: str | None = None,
                           min_confidence: float | None = None,
                           setup: str | None = None,
                           since_hours: int | None = None):
    rows = await api_signals(request, repo, limit=limit, symbol=symbol,
                             min_confidence=min_confidence, setup=setup,
                             since_hours=since_hours)
    return templates.TemplateResponse(
        request, "fragments/signals_table.html", {"signals": rows},
    )


@api_router.get("/fragments/coins")
async def fragment_coins(request: Request,
                         repo: Repository = Depends(get_repo)):
    rows = await api_per_symbol(repo)
    return templates.TemplateResponse(
        request, "fragments/coins_table.html", {"coins": rows},
    )


@api_router.get("/fragments/patterns")
async def fragment_patterns(request: Request,
                            repo: Repository = Depends(get_repo)):
    rows = await api_per_setup(repo)
    return templates.TemplateResponse(
        request, "fragments/patterns_table.html", {"patterns": rows},
    )


# ---------- Paper trading ----------


class PaperStrategyIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    deposit: float = Field(default=1000.0, ge=0.0)
    size_pct: float = Field(default=10.0, gt=0.0, le=100.0)
    sl_mult: float = Field(default=1.0, gt=0.0, le=10.0)
    tp1_mult: float = Field(default=1.0, gt=0.0, le=10.0)
    tp2_mult: float = Field(default=1.0, gt=0.0, le=10.0)
    min_confidence: float = Field(default=55.0, ge=0.0, le=100.0)
    active: bool = True


@api_router.get("/paper/strategies")
async def api_paper_strategies(repo: Repository = Depends(get_repo)):
    rows = await repo.paper_strategy_summary()
    return rows


@api_router.get("/paper/strategies/{strategy_id}")
async def api_paper_strategy_get(
    strategy_id: int,
    repo: Repository = Depends(get_repo),
):
    row = await repo.get_paper_strategy(strategy_id)
    if not row:
        raise HTTPException(status_code=404, detail="strategy not found")
    return row


@api_router.post("/paper/strategies")
async def api_paper_strategies_create(
    payload: PaperStrategyIn,
    repo: Repository = Depends(get_repo),
):
    data = payload.model_dump()
    data["active"] = 1 if data.pop("active") else 0
    try:
        new_id = await repo.insert_paper_strategy(data)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"db_error: {exc!r}") from exc
    return {"id": new_id, **data}


@api_router.put("/paper/strategies/{strategy_id}")
async def api_paper_strategies_update(
    strategy_id: int,
    payload: PaperStrategyIn,
    repo: Repository = Depends(get_repo),
):
    existing = await repo.get_paper_strategy(strategy_id)
    if not existing:
        raise HTTPException(status_code=404, detail="strategy not found")
    data = payload.model_dump()
    data["active"] = 1 if data.pop("active") else 0
    try:
        await repo.update_paper_strategy(strategy_id, data)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"db_error: {exc!r}") from exc
    return {"id": strategy_id, **data}


@api_router.delete("/paper/strategies/{strategy_id}")
async def api_paper_strategies_delete(
    strategy_id: int,
    repo: Repository = Depends(get_repo),
):
    existing = await repo.get_paper_strategy(strategy_id)
    if not existing:
        raise HTTPException(status_code=404, detail="strategy not found")
    await repo.delete_paper_strategy(strategy_id)
    return {"deleted": strategy_id}


@api_router.patch("/paper/strategies/{strategy_id}/toggle")
async def api_paper_strategies_toggle(
    strategy_id: int,
    repo: Repository = Depends(get_repo),
):
    existing = await repo.get_paper_strategy(strategy_id)
    if not existing:
        raise HTTPException(status_code=404, detail="strategy not found")
    new_active = await repo.toggle_paper_strategy(strategy_id)
    return {"id": strategy_id, "active": new_active}


@api_router.get("/paper/trades")
async def api_paper_trades(
    repo: Repository = Depends(get_repo),
    strategy_id: int | None = None,
    status: str | None = None,
    symbol: str | None = None,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return await repo.list_paper_trades(
        strategy_id=strategy_id,
        status=status,
        symbol=symbol,
        limit=limit,
        offset=offset,
    )


@api_router.get("/paper/equity")
async def api_paper_equity(
    repo: Repository = Depends(get_repo),
    strategy_id: int | None = None,
):
    curve = await repo.paper_equity_curve(strategy_id=strategy_id)
    out = []
    for sid, info in curve.items():
        out.append({
            "strategy_id": sid,
            "name": info["name"],
            "deposit": info["deposit"],
            "points": info["points"],
        })
    return out


@api_router.get("/paper/summary")
async def api_paper_summary(repo: Repository = Depends(get_repo)):
    rows = await repo.paper_strategy_summary()
    total_realized = sum(float(r.get("realized_pnl") or 0.0) for r in rows)
    total_deposit = sum(float(r.get("deposit") or 0.0) for r in rows)
    return {
        "strategies": rows,
        "total_realized_pnl": round(total_realized, 4),
        "total_deposit": round(total_deposit, 4),
        "total_balance": round(total_deposit + total_realized, 4),
    }


@api_router.get("/fragments/paper/strategies")
async def fragment_paper_strategies(
    request: Request,
    repo: Repository = Depends(get_repo),
):
    rows = await repo.paper_strategy_summary()
    return templates.TemplateResponse(
        request, "fragments/paper_strategies.html", {"strategies": rows},
    )


@api_router.get("/fragments/paper/open")
async def fragment_paper_open(
    request: Request,
    repo: Repository = Depends(get_repo),
):
    trades = await repo.list_paper_trades(status="open", limit=200)
    # Try to enrich with current price = last close from signals row.
    return templates.TemplateResponse(
        request, "fragments/paper_open.html", {"trades": trades},
    )


@api_router.get("/fragments/paper/history")
async def fragment_paper_history(
    request: Request,
    repo: Repository = Depends(get_repo),
    strategy_id: int | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    trades = await repo.list_paper_trades(
        strategy_id=strategy_id, status="closed", limit=limit
    )
    return templates.TemplateResponse(
        request, "fragments/paper_history.html", {"trades": trades},
    )


# ---------- Bot settings ----------


class BotSettingIn(BaseModel):
    value: str = Field(max_length=256)


@api_router.get("/settings")
async def api_get_settings(repo: Repository = Depends(get_repo)):
    return await repo.list_bot_settings()


@api_router.put("/settings/{key}")
async def api_set_setting(
    key: str,
    payload: BotSettingIn,
    repo: Repository = Depends(get_repo),
):
    if len(key) > 64 or not key.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="invalid key")
    await repo.set_bot_setting(key, payload.value)
    return {"key": key, "value": payload.value}
