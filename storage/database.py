"""Async SQLite layer (aiosqlite) with WAL, conservative tuning, and a small
repository facade for the rest of the codebase.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

import aiosqlite

from utils import now_ms

log = logging.getLogger("storage")

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


class Database:
    """Thin connection wrapper.

    A single shared aiosqlite connection is used. SQLite's GIL-equivalent
    serializes writes anyway, and a single connection drops the per-connection
    page cache footprint substantially.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(
            self.path,
            isolation_level=None,  # autocommit; we manage transactions explicitly
            timeout=10.0,
        )
        # Conservative pragmas for a 1GB VPS:
        # - WAL: avoids reader/writer contention without huge buffers
        # - synchronous=NORMAL: safe with WAL, ~10x faster commits
        # - cache_size negative => kilobytes; 8MB cache is plenty
        # - temp_store=MEMORY: small temp ops in RAM (still small)
        await self._exec("PRAGMA journal_mode=WAL")
        await self._exec("PRAGMA synchronous=NORMAL")
        await self._exec("PRAGMA temp_store=MEMORY")
        await self._exec("PRAGMA cache_size=-8000")
        await self._exec("PRAGMA mmap_size=33554432")  # 32MB mmap
        await self._exec("PRAGMA foreign_keys=ON")

        schema = _SCHEMA_PATH.read_text()
        async with self._lock:
            await self._conn.executescript(schema)
        log.info("storage_ready", extra={"path": str(self.path)})

    async def close(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            finally:
                self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected; call .connect() first")
        return self._conn

    async def _exec(self, sql: str, params: Iterable[Any] | None = None) -> None:
        await self.conn.execute(sql, params or ())

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._lock:
            await self._exec("BEGIN")
            try:
                yield self.conn
                await self._exec("COMMIT")
            except Exception:
                await self._exec("ROLLBACK")
                raise

    # ---------- helpers ----------

    async def fetch_all(self, sql: str, params: Iterable[Any] | None = None) -> list[aiosqlite.Row]:
        self.conn.row_factory = aiosqlite.Row
        async with self.conn.execute(sql, params or ()) as cur:
            return await cur.fetchall()

    async def fetch_one(self, sql: str, params: Iterable[Any] | None = None) -> aiosqlite.Row | None:
        self.conn.row_factory = aiosqlite.Row
        async with self.conn.execute(sql, params or ()) as cur:
            return await cur.fetchone()

    async def execute(self, sql: str, params: Iterable[Any] | None = None) -> int:
        async with self._lock:
            cur = await self.conn.execute(sql, params or ())
            try:
                return cur.lastrowid or 0
            finally:
                await cur.close()

    async def executemany(self, sql: str, rows: Iterable[Iterable[Any]]) -> None:
        async with self._lock:
            await self.conn.executemany(sql, list(rows))


def _to_json(obj: Any) -> str:
    if obj is None:
        return "null"
    if isinstance(obj, str):
        return obj
    try:
        return json.dumps(obj, default=str, separators=(",", ":"), ensure_ascii=False)
    except TypeError:
        return json.dumps(asdict(obj), default=str, separators=(",", ":"), ensure_ascii=False)


class Repository:
    """High-level data access — engine + dashboard use only this class."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ----- symbols -----

    async def upsert_symbols(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        ts = now_ms()
        await self.db.executemany(
            """
            INSERT INTO symbols(symbol, quote_asset, last_price, quote_volume_24h,
                                price_change_pct, active, updated_ms)
            VALUES(:symbol, :quote_asset, :last_price, :quote_volume_24h,
                   :price_change_pct, :active, :updated_ms)
            ON CONFLICT(symbol) DO UPDATE SET
                last_price=excluded.last_price,
                quote_volume_24h=excluded.quote_volume_24h,
                price_change_pct=excluded.price_change_pct,
                active=excluded.active,
                updated_ms=excluded.updated_ms
            """,
            [{"updated_ms": ts, **r} for r in rows],
        )

    async def list_active_symbols(self) -> list[dict[str, Any]]:
        rows = await self.db.fetch_all(
            "SELECT * FROM symbols WHERE active=1 ORDER BY quote_volume_24h DESC"
        )
        return [dict(r) for r in rows]

    # ----- pumps -----

    async def insert_pump(self, row: dict[str, Any]) -> int:
        row = dict(row)
        row.setdefault("detected_ms", now_ms())
        row["reasons_json"] = _to_json(row.pop("reasons", []))
        row["metadata_json"] = _to_json(row.pop("metadata", {}))
        return await self.db.execute(
            """
            INSERT INTO pumps(
                symbol, detected_ms, price, pump_5m_pct, pump_15m_pct,
                volume_ratio, oi_change_pct, funding_rate, pump_score,
                relative_btc, reasons_json, metadata_json
            ) VALUES (
                :symbol, :detected_ms, :price, :pump_5m_pct, :pump_15m_pct,
                :volume_ratio, :oi_change_pct, :funding_rate, :pump_score,
                :relative_btc, :reasons_json, :metadata_json
            )
            """,
            row,
        )

    # ----- signals -----

    async def insert_signal(self, row: dict[str, Any]) -> int:
        row = dict(row)
        row.setdefault("created_ms", now_ms())
        row["reasons_json"] = _to_json(row.pop("reasons", []))
        row["setup_tags_json"] = _to_json(row.pop("setup_tags", []))
        row["metadata_json"] = _to_json(row.pop("metadata", {}))
        return await self.db.execute(
            """
            INSERT INTO signals(
                symbol, created_ms, price, pump_5m_pct, pump_15m_pct,
                pump_score, exhaustion_score, fake_pump_score, confidence_score,
                confidence_label, market_regime, volume_ratio, oi_change_pct,
                funding_rate, suggested_entry, suggested_sl, suggested_tp1,
                suggested_tp2, reasons_json, setup_tags_json, metadata_json
            ) VALUES (
                :symbol, :created_ms, :price, :pump_5m_pct, :pump_15m_pct,
                :pump_score, :exhaustion_score, :fake_pump_score, :confidence_score,
                :confidence_label, :market_regime, :volume_ratio, :oi_change_pct,
                :funding_rate, :suggested_entry, :suggested_sl, :suggested_tp1,
                :suggested_tp2, :reasons_json, :setup_tags_json, :metadata_json
            )
            """,
            row,
        )

    async def last_signal_for(self, symbol: str) -> dict[str, Any] | None:
        row = await self.db.fetch_one(
            "SELECT * FROM signals WHERE symbol=? ORDER BY created_ms DESC LIMIT 1",
            (symbol,),
        )
        return dict(row) if row else None

    async def list_signals(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        symbol: str | None = None,
        min_confidence: float | None = None,
        setup_tag: str | None = None,
        since_ms: int | None = None,
        until_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        where = []
        params: list[Any] = []
        if symbol:
            where.append("symbol = ?")
            params.append(symbol.upper())
        if min_confidence is not None:
            where.append("confidence_score >= ?")
            params.append(min_confidence)
        if setup_tag:
            where.append("setup_tags_json LIKE ?")
            params.append(f'%"{setup_tag}"%')
        if since_ms is not None:
            where.append("created_ms >= ?")
            params.append(since_ms)
        if until_ms is not None:
            where.append("created_ms <= ?")
            params.append(until_ms)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        sql = (
            f"SELECT s.*, o.max_favorable_pct, o.max_adverse_pct, o.final_pct, "
            f"o.is_win, o.invalidated, o.hit_tp1, o.hit_tp2, o.hit_sl, o.closed_ms "
            f"FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id "
            f"{clause} ORDER BY s.created_ms DESC LIMIT ? OFFSET ?"
        )
        params.extend([limit, offset])
        rows = await self.db.fetch_all(sql, params)
        return [dict(r) for r in rows]

    # ----- outcomes -----

    async def upsert_outcome(self, signal_id: int, row: dict[str, Any]) -> None:
        row = dict(row)
        row["signal_id"] = signal_id
        await self.db.execute(
            """
            INSERT INTO signal_outcomes(
                signal_id, tracked_until_ms, max_favorable_pct, max_adverse_pct,
                reversal_speed_sec, final_pct, hit_tp1, hit_tp2, hit_sl,
                invalidated, is_win, closed_ms
            ) VALUES (
                :signal_id, :tracked_until_ms, :max_favorable_pct, :max_adverse_pct,
                :reversal_speed_sec, :final_pct, :hit_tp1, :hit_tp2, :hit_sl,
                :invalidated, :is_win, :closed_ms
            )
            ON CONFLICT(signal_id) DO UPDATE SET
                tracked_until_ms=excluded.tracked_until_ms,
                max_favorable_pct=excluded.max_favorable_pct,
                max_adverse_pct=excluded.max_adverse_pct,
                reversal_speed_sec=excluded.reversal_speed_sec,
                final_pct=excluded.final_pct,
                hit_tp1=excluded.hit_tp1,
                hit_tp2=excluded.hit_tp2,
                hit_sl=excluded.hit_sl,
                invalidated=excluded.invalidated,
                is_win=excluded.is_win,
                closed_ms=excluded.closed_ms
            """,
            row,
        )

    async def open_outcomes(self) -> list[dict[str, Any]]:
        rows = await self.db.fetch_all(
            """
            SELECT s.id, s.symbol, s.created_ms, s.price, s.suggested_sl,
                   s.suggested_tp1, s.suggested_tp2
            FROM signals s
            LEFT JOIN signal_outcomes o ON o.signal_id=s.id
            WHERE o.signal_id IS NULL OR (o.closed_ms IS NULL)
            ORDER BY s.created_ms DESC
            LIMIT 200
            """
        )
        return [dict(r) for r in rows]

    # ----- market snapshots -----

    async def insert_market_snapshot(self, row: dict[str, Any]) -> None:
        row = dict(row)
        row.setdefault("ts_ms", now_ms())
        row["metadata_json"] = _to_json(row.pop("metadata", {}))
        await self.db.execute(
            """
            INSERT INTO market_snapshots(
                ts_ms, btc_price, btc_regime, btc_trend_pct, btc_atr_pct,
                avg_universe_vol, overheated_count, metadata_json
            ) VALUES (
                :ts_ms, :btc_price, :btc_regime, :btc_trend_pct, :btc_atr_pct,
                :avg_universe_vol, :overheated_count, :metadata_json
            )
            """,
            row,
        )

    async def latest_market_snapshot(self) -> dict[str, Any] | None:
        row = await self.db.fetch_one(
            "SELECT * FROM market_snapshots ORDER BY ts_ms DESC LIMIT 1"
        )
        return dict(row) if row else None

    # ----- analytics -----

    async def aggregate_signal_stats(
        self,
        *,
        since_ms: int | None = None,
    ) -> dict[str, Any]:
        where = ""
        params: list[Any] = []
        if since_ms is not None:
            where = "WHERE s.created_ms >= ?"
            params = [since_ms]
        row = await self.db.fetch_one(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN o.is_win=1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN o.is_win=0 THEN 1 ELSE 0 END) AS losses,
                AVG(o.max_favorable_pct) AS avg_mfe,
                AVG(o.max_adverse_pct) AS avg_mae,
                AVG(s.confidence_score) AS avg_conf
            FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
            {where}
            """,
            params,
        )
        return dict(row) if row else {}

    async def per_symbol_stats(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self.db.fetch_all(
            """
            SELECT s.symbol,
                   COUNT(*) AS total,
                   SUM(CASE WHEN o.is_win=1 THEN 1 ELSE 0 END) AS wins,
                   AVG(o.max_favorable_pct) AS avg_mfe,
                   AVG(o.max_adverse_pct) AS avg_mae,
                   AVG(s.confidence_score) AS avg_conf
            FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
            GROUP BY s.symbol
            ORDER BY total DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in rows]

    async def per_setup_stats(self) -> list[dict[str, Any]]:
        # Setup tags are stored JSON-encoded; do the JOIN-via-LIKE for speed.
        tags = [
            "failed_breakout",
            "rsi_divergence",
            "volume_climax",
            "oi_flat",
            "aggressive_sell",
            "liquidity_grab",
            "blowoff_top",
            "cvd_divergence",
        ]
        out: list[dict[str, Any]] = []
        for tag in tags:
            row = await self.db.fetch_one(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN o.is_win=1 THEN 1 ELSE 0 END) AS wins,
                       AVG(o.max_favorable_pct) AS avg_mfe,
                       AVG(o.max_adverse_pct) AS avg_mae,
                       AVG(s.confidence_score) AS avg_conf
                FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                WHERE setup_tags_json LIKE ?
                """,
                (f'%"{tag}"%',),
            )
            d = dict(row) if row else {}
            d["tag"] = tag
            out.append(d)
        return out

    async def signals_per_day(self, days: int = 30) -> list[dict[str, Any]]:
        rows = await self.db.fetch_all(
            """
            SELECT strftime('%Y-%m-%d', s.created_ms/1000, 'unixepoch') AS day,
                   COUNT(*) AS total,
                   SUM(CASE WHEN o.is_win=1 THEN 1 ELSE 0 END) AS wins,
                   AVG(s.confidence_score) AS avg_conf
            FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
            GROUP BY day
            ORDER BY day DESC
            LIMIT ?
            """,
            (days,),
        )
        return list(reversed([dict(r) for r in rows]))


async def open_database(path: str | Path) -> Database:
    db = Database(path)
    await db.connect()
    return db
