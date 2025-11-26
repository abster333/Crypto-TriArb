# DEX V5 Stack Design Brief

## Executive Summary

Design and implement a V5 DEX ingestion stack that decouples from the legacy CEX stack, prioritizes observability as a core principle, and maintains an explicit path toward triangular arbitrage execution. Reuse proven V4 components while replacing flawed subsystems.

---

## Current State (V4)

### Strengths
1. **Batching Server** – Reliable, handles WebSocket multiplexing across connection limits effectively
2. **WebSocket Foundation** – Generally stable; could benefit from explicit fallbacks and error recovery
3. **Manifest Generation** – Robust pool/pair discovery; don't refactor
4. **CCXT Integration** – Python microservice (`python-ingest/`) works well for CEX polling

### Weaknesses
1. **Coupling** – V4 is deeply entangled with centralized exchange (CEX) stack; unclear configuration flags and environment variable sprawl
2. **Math/Simulation Issues**
   - Pool state snapshots not normalized consistently
   - Cycle simulation produces unrealistic ROI values (both extremely positive and extremely negative)
   - Quote calculation (swap amounts) returns bad values
   - Cycle ROI calculations diverge significantly from reality
3. **Observability Gaps**
   - Visibility tooling exists but is crude—many "gaps" requiring post-hoc debugging
   - Missing metrics for data normalization quality, WebSocket health, snapshot freshness
   - No pool-level price dashboard (critical for validation)
4. **Extensibility** – Hard-coded for PancakeSwap (BSC) + Uniswap (Ethereum); difficult to add new chains/DEX protocols

---

## V5 Vision

### Design Principles
1. **Visibility-First Architecture**
   - Every data transformation is observable
   - Pool snapshots include normalization quality metrics
   - WebSocket health (latency, reconnects, message loss) is tracked in real time
   - Pool price feeds validated against external sources
   
2. **Single Responsibility**
   - V5 owns only DEX ingestion, math, and simulation
   - No CEX data—delegated to `python-ingest/`
   - No order execution logic initially (phase 2+)
   
3. **Extensibility from Day One**
   - Plugin architecture for new chains (BSC, Ethereum, Polygon, etc.)
   - Pluggable DEX implementations (Uniswap V2/V3, PancakeSwap, etc.)
   - Common interface for pool state and swap calculation
   
4. **Clear End Goal**
   - Every feature serves triangular arbitrage: gather data → simulate → execute
   - No scope creep into features not directly supporting this goal

---

## V5 Implementation Roadmap

### Phase 1: Data Gathering + Observability (Priority 1)

**Goal:** Ingest and validate all pool data with full visibility into quality.

**Deliverables:**
1. **Pool Manifest & Discovery**
   - Reuse V4's manifest generation
   - Add checksums to track schema changes
   - Log discovery results: pools found, version mismatches, skipped pairs

2. **Snapshot Pipeline**
   - Fetch pool state (reserve amounts, fees, decimals) from RPC
   - Snapshot format: normalized (human-readable amounts + decimals reconciled)
   - Per-snapshot quality metrics: RPC latency, consistency checks, last-update timestamp
   - Publish to NATS `dex.pool.snapshot` + Redis `dex:pool:{chainId}:{poolId}`

3. **WebSocket Feed**
   - Establish WebSocket listeners for swap events (Uniswap V3 logs, PancakeSwap events)
   - Reconnection + backoff strategy
   - Metrics: uptime, message rate, reconnect count, lastMessageTime
   - Fallback to RPC polling if WS stale

4. **Dashboards (v0.1)**
   - Pool health: count of active snapshots, freshness heatmap, error rate per chain
   - WebSocket status: latency per provider, reconnect events, message frequency
   - Data quality: normalization errors flagged, stale snapshots detected
   - Price validation: compare Uniswap reserves against external DEX prices (optional: DEX aggregator API)

5. **Logging & Metrics**
   - Structured logs (JSON) with traceId for request correlation
   - Prometheus metrics: snapshot latency, pool discovery rate, data quality %
   - Alert thresholds: no snapshots in 5 min, WS disconnected >2 min, 10%+ quality failures

### Phase 2: Simulation Engine (Priority 2)

**Goal:** Accurate swap quote and cycle ROI calculation with full traceability.

**Deliverables:**
1. **Quote Calculation**
   - Implement Uniswap V3 tick math correctly (no shortcuts—test against on-chain calldata)
   - PancakeSwap V2 swap formula (x*y=k with fees)
   - Log every intermediate calculation: input amount → output amount with fee deductions
   - Test harness comparing local calculations vs. contract calls

2. **Cycle Detection**
   - Find all possible triangular paths (A→B→C→A) using graph traversal
   - Filter by minimum volume thresholds
   - Reject cycles with >10% slippage (configurable)
   - Log rejected cycles with reason

3. **Cycle ROI Calculation**
   - For each cycle: start amount → simulate each leg → final amount
   - ROI = (final - start) / start
   - Log intermediate ROI per leg; identify where value is lost
   - Compare against mev-inspect or other public cycle simulators

4. **Dashboards (v0.2)**
   - Simulation audit: list all cycles with leg-by-leg quotes + cumulative ROI
   - Quote accuracy: compare local calc vs. on-chain for random sample of swaps
   - Cycle profitability: heatmap of margins by pair combo and pool size

5. **Testing**
   - Unit tests for tick math (Uniswap V3), swap formula (all protocols)
   - Integration tests: fetch live pool snapshots → simulate → compare known ROI
   - Property tests: ROI must be monotonic w.r.t. volume (or explain why)

### Phase 3: Order Execution (Priority 3)

**Goal:** Route profitable cycles to chain and execute with monitoring.

**Deliverables:**
1. **Execution Planner**
   - Given a profitable cycle + wallet state, plan atomic swap
   - Bundle detection: identify flashbot-friendly transactions
   - Slippage estimate from simulation + MEV buffer
   
2. **Order Submission**
   - Send transactions to chain (initially with high-level handlers, no routing logic)
   - Mempool monitoring to detect failures/evictions
   
3. **Dashboards (v0.3)**
   - Trade log: executed cycles with on-chain confirmation times
   - Profit/loss: simulated ROI vs. actual realized P&L
   - Execution errors: mempool rejections, reverts, timeout events

---

## V5 Architecture (Proposed)

```
dex-v5/
├── Makefile                    # Dev commands: setup, run, test, lint
├── pyproject.toml              # Dependencies
├── README.md
├── src/
│   ├── core/
│   │   ├── models.py           # Pool, Snapshot, Cycle, Quote dataclasses
│   │   ├── errors.py           # Custom exceptions with traceId
│   │   └── logger.py           # Structured logging
│   ├── chains/                 # Chain drivers
│   │   ├── base.py             # IChainProvider interface
│   │   ├── ethereum.py
│   │   ├── bsc.py
│   │   └── polygon.py
│   ├── dexes/                  # DEX implementations
│   │   ├── base.py             # IPoolMath interface
│   │   ├── uniswap_v3.py
│   │   ├── uniswap_v2.py
│   │   └── pancakeswap.py
│   ├── ingest/
│   │   ├── snapshot.py         # RPC-based snapshots
│   │   ├── websocket.py        # WS event streams
│   │   └── discovery.py        # Pool enumeration
│   ├── simulation/
│   │   ├── quotes.py           # Swap quote calculation
│   │   ├── cycles.py           # Cycle detection & ROI
│   │   └── auditor.py          # Simulation verification
│   ├── execution/              # (Phase 3)
│   │   ├── planner.py
│   │   └── submitter.py
│   ├── observability/
│   │   ├── metrics.py          # Prometheus metrics
│   │   ├── tracing.py          # Request/cycle tracing
│   │   └── dashboards.py       # UI data endpoints
│   └── main.py                 # FastAPI entrypoint
├── tests/
│   ├── unit/
│   │   ├── test_tick_math.py
│   │   ├── test_swap_formula.py
│   │   └── test_cycle_detection.py
│   └── integration/
│       ├── test_snapshot_quality.py
│       └── test_quote_accuracy.py
├── scripts/
│   ├── compare_with_mev_inspect.py
│   ├── backtest_cycles.py
│   └── generate_pool_manifest.py
└── docker/
    ├── Dockerfile
    └── docker-compose.yml
```

---

## V5 API & Data Contracts

### NATS Subjects
- `dex.pool.snapshot` → `PoolSnapshot` (RPC-sourced)
- `dex.pool.event` → `SwapEvent` (WS-sourced)
- `dex.cycle.simulated` → `SimulatedCycle` (with leg details + ROI)
- `dex.cycle.executed` → `ExecutionResult` (Phase 3)

### REST Endpoints
- `GET /health` – service readiness + last snapshot time
- `GET /pools` – list active pools, freshness, chain
- `GET /pools/{poolId}/price` – current price + confidence
- `GET /cycles` – all discovered cycles with ROI
- `POST /cycles/simulate` – simulate a custom cycle
- `GET /metrics` – Prometheus scrape endpoint

### Redis Keys
- `dex:pool:{chainId}:{poolId}` → pool state (TTL: 10s)
- `dex:cycle:{cycleId}` → best ROI for cycle (TTL: 5s)
- `dex:metrics:{key}` → rolling counters (TTL: 1h)

---

## V5 Success Criteria

### Observability
- [ ] Every snapshot logged with quality score + RPC latency
- [ ] WebSocket health dashboard showing uptime + reconnects per provider
- [ ] Pool price validated against external source (alerting on 5%+ discrepancy)
- [ ] All cycle simulations auditable: log input pool states → quotes → final ROI

### Math Correctness
- [ ] Uniswap V3 quotes match on-chain calls within 0.1 basis points
- [ ] Cycle ROI reproducible: same pool states → same ROI (within rounding)
- [ ] Detected cycles have positive net profit (after routing fees)

### Extensibility
- [ ] Adding a new chain requires <1 day: implement IChainProvider + tests
- [ ] Adding a new DEX protocol requires <2 days: implement IPoolMath + tests

### Performance
- [ ] Snapshot latency: P99 <1s (from RPC call start → NATS publish)
- [ ] Cycle detection: all pools scanned + ROI sorted in <2s
- [ ] WebSocket: message processing latency <100ms

---

## Reusable V4 Components

1. **Manifest Generation** (`scripts/build_dex_v4_manifest.py`)
   - Use as-is; add version + checksum tracking
   
2. **Batching Server** (Java; optional)
   - Can batch RPC calls for multi-pool snapshots
   - May not be necessary if Python `web3.py` + `aiohttp` is sufficient
   
3. **CCXT Integration**
   - Keep `python-ingest/` isolated; V5 can optionally correlate CEX prices for validation

---

## Getting Started

1. **Code Review** – Examine V4 math logic in `src/main/java/.../Arbitrage`, `dex_v4/` simulation code
2. **Spike: Snapshot + Dashboard** – Build Phase 1 foundation with minimal cycle logic
3. **Spike: Quote Calculation** – Verify Uniswap V3 tick math against test vectors
4. **Iterate** – Phase 2 cycle detection, Phase 3 execution with team feedback

---

## Notes

- **No Hard-Coded Config** – All pool addresses, RPC endpoints, and strategy thresholds via env vars or config file
- **Testing First** – Simulation unit tests before integration; compare against known good ROI values
- **Documentation** – Inline docs for complex math; architecture decision records (ADRs) for major choices
- **DevOps** – Docker compose with Redis, NATS, Prometheus; one-command `make dev-up`
