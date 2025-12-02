CryptoTriArb

This branch keeps only the active stacks:
- `cex_triarb_v1/` — Python CEX tri-arb (Coinbase + Kraken) with ingest, dashboard, and strategy.
- `cex_depth_v1/` — depth-only ingest/strategy/UI wrapper around v1 (see quick start below).
- `cex-triarb-v2/` — your v2 iteration (kept as-is).
- `v5/` — DEX-focused stack (see `v5/` and `docs/v5/README.md`).
- Config retained only for v5 under `config/v5/`.

All legacy Java/Spring, old DEX versions, Helm charts, and miscellaneous scripts have been removed.

## Architecture (CEX + DEX)

```mermaid
flowchart LR
    subgraph Brokers
        R[Redis (hot cache)]
        N[NATS]
    end

    subgraph CEX Depth
        CI[Ingest: OKX/Coinbase/Kraken WS\nbatching + prune 60018]
        CS[Strategy: depth arb\nper-leg latency, stale gate]
    end

    subgraph DEX v5
        DI[DEX ingest: on-chain RPC/WS\npool manifests]
        DS[DEX strategies\npath search & sim]
    end

    CI -->|L1/L5 snapshots| R
    CI -->|snapshots| N
    CS -->|opportunities| N
    DI -->|pool state| R
    DS -->|signals/opps| N

    UI[UI / dashboards] --> R
    UI --> N
```

## Performance & observability
- **Latency captured per leg**: every opportunity includes `latency_ms` (per-leg list, max, avg) derived from `ts_ingest - ts_event`.
- **Staleness gate**: `DEPTH_STALE_MS` (default 500 ms) skips cycles with old books; set high to disable. Latency remains logged either way.
- **Pruning bad markets**: OKX 60018 errors prune those instIds from Redis desired symbols immediately; prevents “waiting for books” noise.
- **Health**: ingest serves an HTTP health server (`DEPTH_HEALTH_PORT`, default 8087); Redis/NATS backoff built in.
- **How to summarize latency/ROI**: `jq -r '.latency_ms.max' logs/depth_opps.ndjson | datamash mean 1 median 1 p90 1` (or pipe through Python) to report your run’s numbers.

## Setup (quick)

1) Deps  
- `pip install -r cex_triarb_v1/requirements.txt` (covers depth and DEX v5).

2) Brokers  
- `docker compose -f cex_triarb_v1/ops/docker-compose.yml up -d nats redis`

3) Env  
- `cp .env.example .env` (v5) and set RPC URLs.  
- `cp .env.depth .env.depth` (already present) and tweak `DEPTH_*` / `STRAT_*` as needed.  
- Load for a shell: `set -a; source .env.depth; set +a`

4) Run CEX depth stack  
- Ingest: `python -m cex_depth_v1.ingest_main`  
- Strategy: `python -m cex_depth_v1.strategy_main`  
- UI: `python -m cex_depth_v1.ui_main` (http://localhost:${DEPTH_UI_PORT:-8090})

5) Run DEX v5 stack  
- `python -m v5.main` (expects `ETHEREUM_RPC_URL` / `WS_URL` etc. from `.env`)

## Tech stack
- Python 3.9+; asyncio, uvloop (optional)
- WebSockets: native `websockets` lib with batching/heartbeat
- Message bus / cache: NATS, Redis hot cache (L1/L5 books, desired symbols)
- Logging/metrics: structured NDJSON logs for opps, per-leg latency; health server via aiohttp
- DEX: on-chain RPC/WS ingest, pool manifest management, multi-hop path simulation
- UI: lightweight Python/JS dashboards (depth, opps, cycles)

## Quick pointers

### cex_triarb_v1
- Deps: `pip install -r cex_triarb_v1/requirements.txt`
- Ingest (WS only by default): `EXCHANGES=COINBASE,KRAKEN python -m cex_triarb_v1.main`
- Dashboard: `python -m cex_triarb_v1.ui_main` (http://localhost:8086)
- Strategy: `python -m cex_triarb_v1.strategy_main`
- Depth mode: set `DEPTH_ENABLED=true DEPTH_LEVELS=10` for ingest, and `STRAT_USE_DEPTH=true` for the strategy to consume depth ladders; dashboard now exposes `/api/depth`.
- Env knobs: see `cex_triarb_v1/README.md`

### cex_depth_v1 (depth-only stack)
- Deps: `pip install -r cex_depth_v1/requirements.txt` (shares v1 deps)
- Bring up brokers: `docker compose -f cex_triarb_v1/ops/docker-compose.yml up -d nats redis`
- Env: `set -a; source .env.depth; set +a` (tweak `DEPTH_*` / `STRAT_*` as needed).
- Ingest: `python -m cex_depth_v1.ingest_main` (supports OKX depth `books5`; Coinbase/Kraken optional).
  - `DEPTH_OKX_BATCH_SIZE` controls how many instIds per WS connection (default 250).
  - Ingest prefilters OKX symbols to `state=live` instruments and prunes any instIds that return OKX 60018 errors, removing them from Redis desired symbols so the strategy won’t wait on them.
  - On startup we clear `md_depth:*` desired_symbols and l1/l5 caches to avoid stale books.
- Strategy: `python -m cex_depth_v1.strategy_main` (auto-builds cycles from Redis desired symbols; emits per-leg fill details and ROI/gross ROI).
  - Per-cycle payload now includes per-leg latency (ts_ingest - ts_event) with max/avg in logs.
  - Freshness gate: `DEPTH_STALE_MS` (default 500 ms) skips cycles with stale books; set to a large value (not 0) to effectively disable the gate.
  - Depth phase toggles via `DEPTH_ENABLE_DEPTH_OPTIMIZATION` / `STRAT_ENABLE_DEPTH_OPTIMIZATION` (defaults to `STRAT_USE_DEPTH`). When enabled, every profitable top-of-book cycle gets an incremental depth walk with iteration-by-iteration ROI, fill sizes, and stop reasons.
- UI: `python -m cex_depth_v1.ui_main` → http://localhost:${DEPTH_UI_PORT:-8090} (shows depth, opps, cycles with per-leg fills/fees and age).
- NATS/Redis connections have retry/backoff; just ensure the brokers are up before starting the stack.
- Fees: default taker bps via env (`FEE_COINBASE_BPS`, `FEE_KRAKEN_BPS`); otherwise uses conservative defaults (Coinbase 6 bps). Retrieve your real Coinbase fee via `/fees` API and export the env var to match.

### v5 (DEX)
- See `v5/` code and `docs/v5/README.md` plus `V5_DESIGN_BRIEF.md`.
- Config files live under `config/v5/`.
- Env now loads from `.env` (and `.devstack-env`); copy `.env.example` and set `ETHEREUM_RPC_URL` / `ETHEREUM_WS_URL` (and BSC if needed). Then run `python -m v5.main`.

## Removed
- Legacy Java app, CCXT Java bridge, old DEX v2/v3/v4 manifests, Helm charts, monitoring assets, and auxiliary Python services (`python-ingest`, `python-strategy-sdk`, etc.).

## Services required
- NATS + Redis (ingest/strategy). Use `cex_triarb_v1/ops/docker-compose.yml` or your own stack.
    --set image.repository=ghcr.io/<you>/crypto-dashboard \
    --set image.tag=<tag> \
    --set env.API_BASE_URL=http://transient-web:8080 \
    --set env.WS_BASE_URL=ws://transient-web:8080/ws/quotes
  ```

One-Command Dev Orchestration
----------------------------

- Prefer `scripts/devctl.sh` for local development. It auto-detects Kubernetes (minikube) vs Docker Compose.
  - `scripts/devctl.sh up` — full bring-up
  - `scripts/devctl.sh status` — fail-fast health smoke (ingest/broker HTTP and Prometheus targets)
  - `scripts/devctl.sh down` — stop/clean
  - See `docs/devctl.md` for more.

To auto-regenerate the shared markets manifest before docker compose comes up, export:

```
export MARKET_MANIFEST_AUTO=true
export MARKET_MANIFEST_EXCHANGES=COINBASE,KRAKEN
```

The generator relies on `ccxt`, so install it once via `pip3 install ccxt`.
