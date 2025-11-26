# CEX Tri-Arb v1 Roadmap (Coinbase + Kraken)

## Milestone 0 — Skeleton (now)
- ✅ Create `cex-triarb-v1` module with isolated ingest adapters (CB/Kraken).
- ✅ Add tech spec + roadmap docs.
- ❑ Wire minimal config + logging scaffolding (env loader, structured logs).

## Milestone 1 — Ingest & Health
- Implement NATS/Redis publishers for snapshots/trades/health.
- Add REST ticker fallback + depth staleness watchdog.
- Control-plane listener for `strategy.ctrl.symbols` to hot-swap subscriptions.
- Expose `/healthz` + `/metrics`; add WS reconnect/lag gauges.
- Smoke scripts: `smoke_ws.sh` (connectivity) and `smoke_cache.sh` (Redis keys).

## Milestone 2 — Strategy (Simulation)
- Build tri-arb detector (top-of-book) over configured cycles.
- Fee + freshness aware ROI; configurable start notional.
- Emit `strategy.opportunity` + NDJSON log; basic dedupe window.
- CLI/UI panel to visualize active opps + recent detections.

## Milestone 3 — Depth & Robustness
- Incorporate L5 depth into fill simulation (notional caps, slip).
- Add replay harness that consumes recorded NDJSON -> NATS for offline tests.
- Expand metrics (calc latency, opps per minute, stale cycle counts).

## Milestone 4 — Persistence & UI Polish
- Optional Timescale/ClickHouse writer for snapshots/opps.
- Lightweight UI (charts for depth age, reconnects, opp timeline).
- Alerting hooks (Prometheus alerts) for feed gaps and stale books.

## Milestone 5 — Execution Readiness (optional)
- Broker interface + paper-trade execution against Coinbase/Kraken sandboxes.
- Risk/limit checks, idempotent order tracking, fill reconciliation.

## Milestone 6 — Hardening
- Load tests (WS and strategy calc), chaos (forced disconnects), soak runs.
- Package & release: docker-compose for NATS+Redis+ingest+strategy+ui, versioned images.
