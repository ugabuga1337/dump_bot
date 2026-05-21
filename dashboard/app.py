"""Lightweight FastAPI dashboard.

Architecture notes:
- Runs as a SEPARATE process from the engine; data flows only via the shared
  SQLite database. The dashboard is mounted read-only by docker-compose.
- Uses Jinja2 + HTMX so we get partial updates without a JS framework.
- Chart.js loaded from CDN (single file, ~70KB gzipped) — no build step.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request

from config import get_settings
from storage import Repository, open_database
from utils import setup_logging

from .api import api_router
from .auth import auth_middleware, login_router
from .templating import templates

log = logging.getLogger("dashboard")


_STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = await open_database(settings.db_path)
        app.state.db = db
        app.state.repo = Repository(db)
        app.state.settings = settings
        app.state.started_at = time.time()
        log.info("dashboard_started",
                 extra={"port": settings.dashboard.port,
                        "user": settings.dashboard.user})
        yield
        await db.close()
        log.info("dashboard_stopped")

    app = FastAPI(
        title="Dump Bot Dashboard",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )

    # IP whitelist + Basic auth via middleware.
    app.middleware("http")(auth_middleware)

    app.include_router(login_router)
    app.include_router(api_router, prefix="/api")

    from fastapi.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    # ---------- pages ----------

    @app.get("/")
    async def root(request: Request):
        return templates.TemplateResponse(
            request,
            "live.html",
            {"section": "live"},
        )

    @app.get("/signals")
    async def signals_page(request: Request):
        return templates.TemplateResponse(
            request, "signals.html", {"section": "signals"},
        )

    @app.get("/analytics")
    async def analytics_page(request: Request):
        return templates.TemplateResponse(
            request, "analytics.html", {"section": "analytics"},
        )

    @app.get("/coins")
    async def coins_page(request: Request):
        return templates.TemplateResponse(
            request, "coins.html", {"section": "coins"},
        )

    @app.get("/patterns")
    async def patterns_page(request: Request):
        return templates.TemplateResponse(
            request, "patterns.html", {"section": "patterns"},
        )

    @app.get("/healthz")
    async def healthz():
        return {"ok": True, "uptime": int(time.time() - app.state.started_at)}

    return app


def main() -> int:
    settings = get_settings()
    app = create_app()
    uvicorn.run(
        app,
        host=settings.dashboard.host,
        port=settings.dashboard.port,
        log_level=settings.log_level.lower() if isinstance(settings.log_level, str) else "info",
        access_log=False,
        proxy_headers=settings.dashboard.behind_proxy,
        forwarded_allow_ips="*" if settings.dashboard.behind_proxy else None,
        workers=1,
        loop="auto",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
