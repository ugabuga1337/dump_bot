# dump_bot

Production-grade signal engine for **Binance Futures USDT-M**. Tracks the
entire perpetual universe in real time, hunts for vertical retail pumps that
are starting to lose momentum, and pushes high-confidence **SHORT setup**
alerts to Telegram. Ships with a lightweight FastAPI dashboard for live
monitoring + signal analytics.

> Designed for a single 1 vCPU / 1 GB RAM VPS. Pure asyncio, zero pandas,
> no Node front-end build step. `docker compose up -d` and you're done.

---

## What it actually does

1. Streams all liquid USDT-M perpetual contracts via Binance combined
   WebSockets (aggTrades / klines / mark price), plus 60 s open-interest
   polling.
2. Multi-factor **pump detection**: price velocity + acceleration + relative
   volume + OI build + funding + relative strength vs BTC + impulse-candle
   structure. Coins that pass move to **WATCH** mode.
3. While in WATCH, runs a weighted **exhaustion scorer**: long upper wicks,
   failed breakouts / continuations, rejection candles, liquidity grabs,
   CVD divergence, delta flips, volume climax / blowoff, OI flattening,
   volatility compression, RSI / VWAP confirmations.
4. **Fake pump detector** penalizes confidence on thin-liquidity / wick
   driven moves that look like manipulation.
5. **Confidence scorer** blends pump + exhaustion + fake-pump penalty +
   BTC regime + whale activity into a 0–100 score with LOW / MED / HIGH
   labels.
6. **Anti-spam** state machine: per-symbol cooldown, duplicate-state
   suppression, system-wide rate cap.
7. **Telegram** alerts with reasons, suggested entry/SL/TP zones, regime.
8. **Paper analysis tracker** records MFE / MAE / reversal speed /
   invalidation per signal — feeds the dashboard analytics.
9. **Lightweight dashboard** (FastAPI + Jinja2 + HTMX + Chart.js).

---

## Project layout

```
dump_bot/
├── main.py                # engine entrypoint
├── config/                # .env loader, YAML weight overrides
├── connectors/            # Binance REST + multiplexed WS
├── core/                  # engine, state, universe, regime, watchdog, models
├── strategy/              # features, indicators, orderflow, microstructure,
│                          # pump_detector, exhaustion, fake_pump
├── signals/               # scoring, anti_spam, outcome_tracker
├── telegram/              # async notifier + HTML formatter
├── storage/               # aiosqlite schema + repository
├── dashboard/             # FastAPI app, auth, API, templates, static
├── backtesting/           # historical replay engine
├── utils/                 # ringbuffer/deque, EMA, logging, timeutils
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── pyproject.toml
```

---

## Quickstart

```bash
git clone https://github.com/ugabuga1337/dump_bot.git
cd dump_bot
cp .env.example .env
# edit .env: set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DASHBOARD_PASSWORD
docker compose up -d
```

That brings up two containers:

- `dump_bot_engine` — signal engine, no exposed ports
- `dump_bot_dashboard` — FastAPI dashboard on `:8080`

Open `http://<vps-ip>:8080`, sign in with `DASHBOARD_USER` /
`DASHBOARD_PASSWORD`, you'll see live status + Telegram health + signal
history + analytics.

### Local dev (no Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m main          # engine
python -m dashboard     # dashboard (separate shell)
pytest -q               # tests
```

---

## Configuration

All settings live in `.env`. The most important knobs:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MIN_QUOTE_VOLUME_24H` | `30000000` | Skip illiquid coins. |
| `MAX_SYMBOLS` | `120` | Hard cap on the active universe (keep RAM small). |
| `BLACKLIST` | `BTCUSDT,ETHUSDT,...` | Never short these. |
| `PUMP_MIN_5M_PCT` / `PUMP_MIN_15M_PCT` | `4.0` / `7.0` | Pump gate. |
| `PUMP_MIN_VOL_RATIO` | `3.0` | Volume spike vs baseline. |
| `PUMP_MIN_OI_PCT` | `2.5` | OI build threshold. |
| `PUMP_SCORE_THRESHOLD` | `55` | Move to WATCH at this score. |
| `EXHAUSTION_SCORE_THRESHOLD` | `65` | Minimum exhaustion to fire. |
| `CONFIDENCE_LOW/MED/HIGH` | `55/70/82` | Labels for Telegram. |
| `WATCH_TTL_SEC` | `900` | Drop WATCH state after this. |
| `COOLDOWN_SEC` | `1800` | Per-symbol post-signal cooldown. |
| `OUTCOME_TRACK_SEC` | `3600` | Paper-analysis window. |
| `OUTCOME_INVALIDATION_PCT` | `2.0` | Adverse move that invalidates a setup. |

Advanced **weight overrides** live in `config/default.yaml`. Tweak
`pump_weights`, `exhaustion_weights`, `fake_pump_weights`,
`confidence_weights`, and `min_confirmations` to change scoring behavior
without touching code.

---

## Telegram setup

1. Talk to [@BotFather](https://t.me/botfather) → `/newbot` → grab the
   token. Set `TELEGRAM_BOT_TOKEN=...`.
2. Open the bot, send `/start`, then forward a message from your target
   chat to [@userinfobot](https://t.me/userinfobot). Set
   `TELEGRAM_CHAT_ID=...` (the chat where alerts should land).
3. If `TELEGRAM_BOT_TOKEN` is empty, the engine still runs — alerts just
   aren't sent. Useful for paper-analysis-only / dashboard-only runs.

A test alert format:

```
🚨 SHORT SETUP DETECTED

Symbol: DOGEUSDT
Price: 0.2145
Pump: +4.30% / 5m, +12.40% / 15m
Volume spike: 6.8x
OI change: +18.00%
Funding: 0.042%
Exhaustion: 82/100
Confidence: 🔴 HIGH (78/100)

Reasons:
• failed breakout
• RSI divergence
• volume climax
• OI flattening
• aggressive sell activity

Suggested zones
Entry: 0.2143
Stop:  0.2210
TP1:   0.2055
TP2:   0.1970

Market regime: BTC bear
```

---

## VPS deployment guide (1 CPU / 1 GB)

The compose setup is already tuned for low resource use:

- `MALLOC_ARENA_MAX=2` to avoid glibc fragmentation
- single-connection SQLite with WAL + 8 MB cache + 32 MB mmap
- combined-stream WS sharded at 180 streams per connection
- engine container hard-capped at 600 MB, dashboard at 256 MB

Recommended steps on a fresh Debian/Ubuntu VPS:

```bash
# 1. Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker

# 2. Project
git clone https://github.com/ugabuga1337/dump_bot.git
cd dump_bot
cp .env.example .env
$EDITOR .env

# 3. Start
docker compose up -d

# 4. Logs / sanity check
docker logs -f dump_bot_engine
docker logs -f dump_bot_dashboard
```

### Optional: nginx reverse proxy + Basic IP whitelist

Put `nginx` in front of the dashboard for TLS and IP-allowlisting:

```nginx
server {
    listen 443 ssl http2;
    server_name dump.example.com;
    ssl_certificate     /etc/letsencrypt/live/dump.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/dump.example.com/privkey.pem;

    allow 1.2.3.4;    # your home IP
    deny  all;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Then set `DASHBOARD_BEHIND_PROXY=true` in `.env` so the app honors
`X-Forwarded-For`.

### Persistent state

All durable state (SQLite, heartbeat file) lives under `./data/`. To wipe
and reset:

```bash
docker compose down && rm -rf data/* && docker compose up -d
```

### Watchdog & auto-restart

`restart: unless-stopped` + a healthcheck that watches the `heartbeat`
file. If the engine wedges, Docker restarts it. WebSocket fleet has its
own internal watchdog (`core/watchdog.py`) that rebuilds shards after 90 s
of silence.

---

## Backtesting

```bash
docker compose run --rm engine python scripts/backtest.py DOGEUSDT 1000PEPEUSDT --days 14
```

(works locally too with `python scripts/backtest.py ...`). This pulls
historical klines and replays them through the same pump + exhaustion +
scoring pipeline. Results print as JSON: `total`, `wins`, `losses`, avg
favorable / adverse % per symbol.

Note: backtesting only has klines, not aggTrade / OI streams — orderflow
features fall back to neutral values. Use it for scoring threshold tuning,
not full re-simulation.

---

## Optimization notes / how the RAM budget is met

- **Per-symbol working set ≈ 36 KB** (90 × 1m klines + 600 agg-trade
  prints + scalar EMAs / OI history). 120 symbols → ~4 MB total.
- **One aiosqlite connection** with WAL + 8 MB cache + 32 MB mmap. No
  per-connection allocators piling up.
- **No DataFrames**. Every detector is O(window_size); rolling sums and
  EMAs are incremental.
- **uvloop** is enabled when available (Linux only) — ~20 % CPU
  improvement over asyncio's default loop.
- **WebSocket fleet sharded at 180 streams** per connection — fits in
  Binance's combined URL limits and lets the watchdog rebuild only a
  single shard if one wedges.
- **Open interest polled at 60 s** (Binance has no free WS for OI on
  perps). Concurrent fan-out limited to 8 sockets to avoid TCP churn.
- **Dashboard runs as a separate process** with read-only SQLite mount,
  so the engine never blocks on HTTP requests. Auto-recovery is a Docker
  restart away.

---

## Anti-noise / quality knobs

The system is built to err on the side of fewer, higher-quality signals:

- WATCH mode + exhaustion gate — pump alone never fires a signal.
- Minimum **N independent confirmations** before exhaustion can pass
  threshold (configurable via `min_confirmations` in `config/default.yaml`).
- Fake-pump penalty drags confidence down on thin-liquidity / wick spikes.
- Anti-spam state machine: cooldown + duplicate-signature suppression +
  global rate cap.
- Market-regime multiplier: BTC bear / chop = easier shorts; BTC bull
  = stricter.

---

## Where to take it next

The architecture is intentionally modular to support:

- ML-ranked confidence (replace `ConfidenceScorer.evaluate` with a
  trained model on the same feature vector).
- Adaptive scoring / parameter optimization via the backtester.
- Auto-tuned weights using outcome analytics.
- Portfolio analytics / position sizing layers.
- Deeper orderflow (full order book WS) and semi-HFT setup detection.
- Reinforcement learning loops over the stored outcomes.

Everything detector-side is pure functions over `SymbolStateData`, so
swapping in alternative scorers / detectors is a one-line change in
`core/engine.py`.

---

## License

MIT. Use at your own risk. The signals are paper-only research output —
this engine never sends trades anywhere. Not financial advice.
