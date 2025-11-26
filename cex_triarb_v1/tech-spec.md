# CEX Triangular Arbitrage Stack — v1 (Coinbase + Kraken)

## Goals
- Single, self-contained CEX stack (no legacy overlap).
- Visibility-first: metrics + health for WS/REST, control-plane hooks, replay fixtures.
- Modular: ingest ↔ strategy decoupled via NATS/Redis; hot-swappable symbol set driven by strategy.
- Simulation-first: emit opportunities + simulated fills before touching real orders.

## Architecture
- **Language:** Python 3.11+, asyncio.
- **Transports:** NATS JetStream (events + control), Redis (hot cache + quick history).
- **Folders:**
  - `ingest/`: WS + REST adapters, symbol control-plane, publishers, health/metrics.
  - `strategy/`: tri-arb detector, opportunity publisher, broker stubs (paper/live).
  - `persistence/` (optional): NDJSON writer + Redis history; DB writers later.
  - `ui/`: small status + opportunity feed (FastAPI/WS or thin Next.js front-end).
  - `common/`: schemas, logging, metrics, config loader.

## Data Contracts (initial)
- **Snapshot (L1/L5)** — subject `md.snapshot`:
  - `exchange` (str), `symbol` (str), `ts_event` (ms), `ts_ingest` (ms),
  - `bids`/`asks`: list of `[price, size, cumulative]` (depth configurable),
  - `source`: `ws|rest`, `latency_ms` map.
- **Trade** — subject `md.trade`: `exchange, symbol, price, size, side, ts_event, ts_ingest`.
- **Health** — subject `md.health`: `exchange, channel (ws/rest), status, last_msg_ms, reconnects, errors`.
- **Control** — subject `strategy.ctrl.symbols`: `{symbols: ["BTCUSD", ...]}` to drive dynamic subscriptions.
- **Opportunity** — subject `strategy.opportunity`: cycle id, ROI, notional, legs, fees, sim_id, timestamps.

## Ingest Pipeline (Coinbase + Kraken only)
- WS adapters: `ingest/ws_coinbase.py`, `ingest/ws_kraken.py` (lifted and isolated).
- Runner: `WsIngestManager` manages adapters, supports hot symbol reload on control messages.
- REST fallback: lightweight ticker poller per symbol (to be added).
- Publishers: NATS + Redis writers (to be added); NDJSON capture for replay.
- Metrics: Prometheus counters/gauges (no-op fallback when library absent).
- Watchdogs: depth staleness, reconnect/backoff, gap alerts.

## Strategy (phase 1)
- Top-of-book tri-arb over shared base/quote (USD/USDT to start).
- Fee-aware ROI; freshness guard (`<1s` age); configurable start notional.
- Simulation mode emits opportunity + simulated fill trace; no live orders.

## Observability
- `/healthz` and `/metrics` on ingest + strategy services.
- Live status stream for UI: per-exchange WS/REST state, lag, last depth ts.
- Optional frame capture (`WS_CAPTURE=true`) writes NDJSON for offline tests.

## Extensibility Notes
- Add more venues by dropping new adapters into `ingest/`.
- Swap transports (Redis Streams instead of NATS) behind publisher interfaces.
- Persistence writers are pluggable; Timescale/ClickHouse can slot in later.
