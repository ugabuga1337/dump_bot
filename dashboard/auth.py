"""Dashboard auth: HTTP Basic + cookie session + optional IP whitelist."""

from __future__ import annotations

import base64
import hmac
import ipaddress
import logging
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request, Response
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer

from config import DashboardConfig, get_settings

log = logging.getLogger("dashboard.auth")

login_router = APIRouter()

_PUBLIC_PATHS = ("/static/", "/healthz", "/login", "/logout", "/favicon.ico")


def _serializer(cfg: DashboardConfig) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(cfg.secret, salt="dump_bot.dashboard")


def _ip_allowed(cfg: DashboardConfig, client_ip: str) -> bool:
    if not cfg.ip_whitelist:
        return True
    try:
        ip = ipaddress.ip_address(client_ip)
    except ValueError:
        return False
    for rule in cfg.ip_whitelist:
        try:
            net = ipaddress.ip_network(rule, strict=False)
        except ValueError:
            continue
        if ip in net:
            return True
    return False


def _check_basic_auth(cfg: DashboardConfig, header: str | None) -> bool:
    if not header or not header.lower().startswith("basic "):
        return False
    try:
        raw = base64.b64decode(header.split(None, 1)[1]).decode()
        user, _, password = raw.partition(":")
    except Exception:
        return False
    return hmac.compare_digest(user, cfg.user) and hmac.compare_digest(password, cfg.password)


def _session_valid(cfg: DashboardConfig, request: Request) -> bool:
    cookie = request.cookies.get("dumpbot_session")
    if not cookie:
        return False
    try:
        data = _serializer(cfg).loads(cookie, max_age=12 * 3600)
    except BadSignature:
        return False
    return data == cfg.user


def _make_session_cookie(cfg: DashboardConfig) -> str:
    return _serializer(cfg).dumps(cfg.user)


async def auth_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    cfg: DashboardConfig = get_settings().dashboard
    path = request.url.path
    client_ip = (
        request.client.host if request.client else ""
    )
    # honor X-Forwarded-For when behind proxy
    if get_settings().dashboard.behind_proxy:
        fwd = request.headers.get("X-Forwarded-For", "").split(",")
        if fwd and fwd[0].strip():
            client_ip = fwd[0].strip()

    if not _ip_allowed(cfg, client_ip):
        log.warning("auth_ip_denied", extra={"ip": client_ip, "path": path})
        return Response("Forbidden\n", status_code=403)

    if path.startswith(_PUBLIC_PATHS):
        return await call_next(request)

    # Either valid cookie session or valid Basic header
    if _session_valid(cfg, request):
        return await call_next(request)

    if _check_basic_auth(cfg, request.headers.get("Authorization")):
        response = await call_next(request)
        response.set_cookie(
            "dumpbot_session",
            _make_session_cookie(cfg),
            max_age=12 * 3600,
            httponly=True,
            samesite="lax",
            secure=False,
        )
        return response

    # API requests get a clean 401; pages get redirected to /login
    if path.startswith("/api/"):
        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="dump_bot"'},
        )
    return RedirectResponse("/login")


@login_router.get("/login")
async def login_page(request: Request):
    from .templating import templates

    return templates.TemplateResponse(request, "login.html", {"error": None})


@login_router.post("/login")
async def login_submit(request: Request):
    from .templating import templates

    form = await request.form()
    user = (form.get("user") or "").strip()
    password = (form.get("password") or "").strip()
    cfg = get_settings().dashboard
    if hmac.compare_digest(user, cfg.user) and hmac.compare_digest(password, cfg.password):
        response = RedirectResponse("/", status_code=303)
        response.set_cookie(
            "dumpbot_session",
            _make_session_cookie(cfg),
            max_age=12 * 3600,
            httponly=True,
            samesite="lax",
            secure=False,
        )
        return response
    log.warning("login_failed", extra={"user": user})
    return templates.TemplateResponse(
        request, "login.html", {"error": "Invalid credentials"}, status_code=401,
    )


@login_router.get("/logout")
async def logout(_: Request):
    response = RedirectResponse("/login")
    response.delete_cookie("dumpbot_session")
    return response


def render_login(request: Request) -> Response:
    from .templating import templates

    return templates.TemplateResponse(request, "login.html", {"error": None})
