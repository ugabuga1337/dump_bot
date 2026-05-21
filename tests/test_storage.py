"""Storage smoke test."""

import time

import pytest

from storage import Repository, open_database


@pytest.mark.asyncio
async def test_signal_round_trip(tmp_path):
    db = await open_database(tmp_path / "t.sqlite3")
    repo = Repository(db)
    try:
        await repo.upsert_symbols([{
            "symbol": "DOGEUSDT", "quote_asset": "USDT",
            "last_price": 0.1, "quote_volume_24h": 1e8,
            "price_change_pct": 2.5, "active": 1,
        }])
        sid = await repo.insert_signal({
            "symbol": "DOGEUSDT", "price": 0.10,
            "pump_5m_pct": 6.0, "pump_15m_pct": 9.0,
            "pump_score": 60.0, "exhaustion_score": 80.0,
            "fake_pump_score": 10.0, "confidence_score": 75.0,
            "confidence_label": "HIGH", "market_regime": "BEAR",
            "volume_ratio": 5.0, "oi_change_pct": 8.0, "funding_rate": 0.04,
            "suggested_entry": 0.10, "suggested_sl": 0.105,
            "suggested_tp1": 0.097, "suggested_tp2": 0.094,
            "reasons": ["failed breakout", "RSI divergence"],
            "setup_tags": ["failed_breakout", "rsi_divergence"],
            "metadata": {"x": 1},
        })
        assert sid > 0

        last = await repo.last_signal_for("DOGEUSDT")
        assert last is not None
        assert last["confidence_score"] == 75.0

        rows = await repo.list_signals(symbol="DOGEUSDT", limit=5)
        assert rows and rows[0]["id"] == sid

        # Outcome tracking
        await repo.upsert_outcome(sid, {
            "tracked_until_ms": int(time.time() * 1000) + 3_600_000,
            "max_favorable_pct": 3.4, "max_adverse_pct": 0.8,
            "reversal_speed_sec": 80.0, "final_pct": -2.1,
            "hit_tp1": 1, "hit_tp2": 0, "hit_sl": 0,
            "invalidated": 0, "is_win": 1,
            "closed_ms": int(time.time() * 1000),
        })

        agg = await repo.aggregate_signal_stats()
        assert agg["total"] == 1
        assert agg["wins"] == 1

        per_setup = await repo.per_setup_stats()
        assert any(p["tag"] == "failed_breakout" and p["total"] == 1 for p in per_setup)
    finally:
        await db.close()
