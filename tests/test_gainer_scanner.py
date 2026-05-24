"""Tests for the REST gainer scanner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from config import GainerConfig
from core.gainer_scanner import GainerScanner


def _ticker(rows: list[tuple[str, float]]) -> list[dict[str, str]]:
    return [{"symbol": s, "lastPrice": str(p)} for s, p in rows]


@pytest.mark.asyncio
async def test_first_scan_seeds_snapshots_no_active():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(return_value=_ticker([("AAA", 1.0), ("BBB", 2.0)]))
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)

    s = GainerScanner(cfg, rest, universe=["AAA", "BBB"])
    active = await s.scan()

    assert active == []
    assert s.stats()["snapshots"] == 2


@pytest.mark.asyncio
async def test_gainer_enters_active_set_when_pct_breached():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[
            _ticker([("AAA", 1.0), ("BBB", 2.0)]),
            # AAA +5%, BBB +0.5%
            _ticker([("AAA", 1.05), ("BBB", 2.01)]),
        ]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)

    s = GainerScanner(cfg, rest, universe=["AAA", "BBB"])
    await s.scan()
    active = await s.scan()
    assert active == ["AAA"]


@pytest.mark.asyncio
async def test_drops_after_consecutive_misses():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[
            _ticker([("AAA", 1.0)]),
            _ticker([("AAA", 1.05)]),  # +5% -> active
            _ticker([("AAA", 1.05)]),  # flat -> miss 1
            _ticker([("AAA", 1.05)]),  # miss 2
            _ticker([("AAA", 1.05)]),  # miss 3 -> drop
        ]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)

    s = GainerScanner(cfg, rest, universe=["AAA"])
    await s.scan()
    assert await s.scan() == ["AAA"]
    assert await s.scan() == ["AAA"]
    assert await s.scan() == ["AAA"]
    assert await s.scan() == []


@pytest.mark.asyncio
async def test_protected_symbols_stay_active_despite_misses():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[
            _ticker([("AAA", 1.0)]),
            _ticker([("AAA", 1.05)]),  # active
            _ticker([("AAA", 1.05)]),
            _ticker([("AAA", 1.05)]),
            _ticker([("AAA", 1.05)]),
        ]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)

    s = GainerScanner(cfg, rest, universe=["AAA"])
    await s.scan()
    await s.scan()
    await s.scan(protected=["AAA"])
    await s.scan(protected=["AAA"])
    active = await s.scan(protected=["AAA"])
    assert "AAA" in active


@pytest.mark.asyncio
async def test_top_n_caps_candidates():
    pairs_now = [("S%d" % i, 1.0) for i in range(10)]
    pairs_next = [("S%d" % i, 1.0 + 0.05 * (i + 1)) for i in range(10)]  # all gainers
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[_ticker(pairs_now), _ticker(pairs_next)]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=3, drop_after_cycles=3)

    s = GainerScanner(cfg, rest, universe=[p[0] for p in pairs_now])
    await s.scan()
    active = await s.scan()
    # Best 3 gainers: S9 (+50%), S8, S7
    assert set(active) == {"S9", "S8", "S7"}


@pytest.mark.asyncio
async def test_universe_filter_drops_unknown_symbols():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[
            _ticker([("AAA", 1.0), ("BBB", 1.0)]),
            _ticker([("AAA", 1.05), ("BBB", 1.05)]),
        ]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)
    s = GainerScanner(cfg, rest, universe=["AAA"])  # BBB excluded
    await s.scan()
    active = await s.scan()
    assert active == ["AAA"]


@pytest.mark.asyncio
async def test_scan_failure_keeps_last_active_plus_protected():
    rest = MagicMock()
    rest.ticker_24h = AsyncMock(
        side_effect=[
            _ticker([("AAA", 1.0)]),
            _ticker([("AAA", 1.05)]),
            RuntimeError("boom"),
        ]
    )
    cfg = GainerConfig(scan_interval_sec=90, min_pct=2.0, top_n=5, drop_after_cycles=3)
    s = GainerScanner(cfg, rest, universe=["AAA"])
    await s.scan()
    await s.scan()
    active = await s.scan(protected=["AAA"])
    assert active == ["AAA"]
