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

-- ---------- Paper trading ----------

CREATE TABLE IF NOT EXISTS paper_strategies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    deposit     REAL NOT NULL DEFAULT 1000.0,
    size_pct    REAL NOT NULL DEFAULT 10.0,   -- % of deposit per trade
    sl_mult     REAL NOT NULL DEFAULT 1.0,    -- SL multiplier vs signal
    tp1_mult    REAL NOT NULL DEFAULT 1.0,    -- TP1 multiplier vs signal
    tp2_mult    REAL NOT NULL DEFAULT 1.0,
    min_confidence REAL NOT NULL DEFAULT 55.0,
    active      INTEGER NOT NULL DEFAULT 1,
    created_ms  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id     INTEGER NOT NULL REFERENCES paper_strategies(id) ON DELETE CASCADE,
    signal_id       INTEGER NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    opened_ms       INTEGER NOT NULL,
    closed_ms       INTEGER,
    entry_price     REAL NOT NULL,
    sl_price        REAL NOT NULL,
    tp1_price       REAL NOT NULL,
    tp2_price       REAL NOT NULL,
    size_usd        REAL NOT NULL,
    close_price     REAL,
    close_reason    TEXT,
    pnl_usd         REAL,
    pnl_pct         REAL,
    status          TEXT NOT NULL DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy ON paper_trades(strategy_id, opened_ms DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades(status);
CREATE INDEX IF NOT EXISTS idx_paper_trades_signal ON paper_trades(signal_id);

-- Generic key-value bot settings (e.g. GAINER_*) editable from dashboard.
CREATE TABLE IF NOT EXISTS bot_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_ms  INTEGER NOT NULL
);
