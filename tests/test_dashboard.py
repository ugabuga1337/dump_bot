"""Dashboard smoke test — boots FastAPI app with an empty SQLite DB."""


import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setenv("DASHBOARD_USER", "admin")
    monkeypatch.setenv("DASHBOARD_PASSWORD", "pwtest")
    monkeypatch.setenv("DASHBOARD_SECRET", "this-is-a-long-test-secret-string")
    from config import reset_settings_for_tests
    reset_settings_for_tests()

    from dashboard.app import create_app

    return create_app()


def test_healthz(app):
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["ok"] is True


def test_root_requires_auth(app):
    with TestClient(app) as client:
        # GET /api/status without auth -> 401
        r = client.get("/api/status")
        assert r.status_code == 401
        # Basic auth ok
        r = client.get("/api/status", auth=("admin", "pwtest"))
        assert r.status_code == 200
        assert "regime" in r.json()
