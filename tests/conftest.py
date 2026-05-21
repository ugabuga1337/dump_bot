"""Smoke + unit tests for the pump/exhaustion pipeline."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def env_paths(tmp_path, monkeypatch):
    """Point storage paths at a temp dir so importing config doesn't try /data."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.sqlite3"))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "")
    from config import reset_settings_for_tests

    reset_settings_for_tests()
    yield
    reset_settings_for_tests()
