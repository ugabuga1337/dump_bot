-- Dump Bot SQLite schema.
-- Conservative: WAL journaling, NORMAL sync, careful indexing.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Discovered futures symbols + quality metrics (rebuilt each universe refresh).
CREATE TABLE IF NOT EXISTS symbols (
    symbol           TEXT PRIMARY KEY,
    quote_asset      TEXT NOT NULL,
    last_price       REAL,
    quote_volume_24h REAL,
    price_change_pct REAL,
    active           INTEGER NOT NULL DEFAULT 1,
    updated_ms       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_symbols_active ON symbols(active);

-- Detected pump events (precursor to a possible signal).
CREATE TABLE IF NOT EXISTS pumps (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol         TEXT NOT NULL,
    detected_ms    INTEGER NOT NULL,
    price          REAL NOT NULL,
    pump_5m_pct    REAL,
    pump_15m_pct   REAL,
    volume_ratio   REAL,
    oi_change_pct  REAL,
    funding_rate   REAL,
    pump_score     REAL,
    relative_btc   REAL,
    reasons_json   TEXT NOT NULL DEFAULT '[]',
    metadata_json  TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_pumps_symbol_ts ON pumps(symbol, detected_ms DESC);

-- Final signal events emitted to Telegram.
CREATE TABLE IF NOT EXISTS signals (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol             TEXT NOT NULL,
    created_ms         INTEGER NOT NULL,
    price              REAL NOT NULL,
    pump_5m_pct        REAL,
    pump_15m_pct       REAL,
    pump_score         REAL,
    exhaustion_score   REAL,
    fake_pump_score    REAL,
    confidence_score   REAL,
    confidence_label   TEXT,
    market_regime      TEXT,
    volume_ratio       REAL,
    oi_change_pct      REAL,
    funding_rate       REAL,
    suggested_entry    REAL,
    suggested_sl       REAL,
    suggested_tp1      REAL,
    suggested_tp2      REAL,
    reasons_json       TEXT NOT NULL DEFAULT '[]',
    setup_tags_json    TEXT NOT NULL DEFAULT '[]',
    metadata_json      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, created_ms DESC);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(created_ms DESC);

-- Paper analysis outcome of each signal.
CREATE TABLE IF NOT EXISTS signal_outcomes (
    signal_id           INTEGER PRIMARY KEY,
    tracked_until_ms    INTEGER NOT NULL,
    max_favorable_pct   REAL,
    max_adverse_pct     REAL,
    reversal_speed_sec  REAL,
    final_pct           REAL,
    hit_tp1             INTEGER NOT NULL DEFAULT 0,
    hit_tp2             INTEGER NOT NULL DEFAULT 0,
    hit_sl              INTEGER NOT NULL DEFAULT 0,
    invalidated         INTEGER NOT NULL DEFAULT 0,
    is_win              INTEGER,
    closed_ms           INTEGER,
    FOREIGN KEY(signal_id) REFERENCES signals(id) ON DELETE CASCADE
);

-- Compact rolling market snapshots (BTC regime / market vol).
CREATE TABLE IF NOT EXISTS market_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms           INTEGER NOT NULL,
    btc_price       REAL,
    btc_regime      TEXT,
    btc_trend_pct   REAL,
    btc_atr_pct     REAL,
    avg_universe_vol REAL,
    overheated_count INTEGER,
    metadata_json   TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_ts ON market_snapshots(ts_ms DESC);

-- Aggregate stats (daily counters) — re-derivable but caching keeps dashboard cheap.
CREATE TABLE IF NOT EXISTS stats_daily (
    day_utc        TEXT PRIMARY KEY,
    signals        INTEGER NOT NULL DEFAULT 0,
    wins           INTEGER NOT NULL DEFAULT 0,
    losses         INTEGER NOT NULL DEFAULT 0,
    avg_dump_pct   REAL,
    avg_adverse_pct REAL,
    avg_confidence REAL
);
