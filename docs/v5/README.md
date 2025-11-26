V5 DEX Arbitrage Detection Stack
================================

1) Overview
-----------

- Async Python pipeline for Uniswap V3 / Pancake V3 triangular arbitrage discovery (dry‑run only).
- Core modules:
  - `v5/common/`: shared Pydantic models, config, metrics, JSON schemas.
  - `v5/state_store/`: Redis hot cache + snapshot store, manifest validation.
  - `v5/ingest/`: RPC pollers, WebSocket adapters, log‑driven WS runner, ingest orchestrator.
  - `v5/simulator/`: token graph, Uniswap V3 math, ROI calculation, realtime arb detector.
  - `v5/visibility/`: metrics exporter, health tracking, alarms, dashboard server.
  - `v5/tools/`: utility scripts (`observe.py`, `ws_probe.py`, `diagnose_pipeline.py`).
- Design goals:
  - Visibility‑first: metrics, health summaries, detailed logging.
  - Event‑driven where possible: WS updates drive state; RPC used for resync/fallback.
  - Safety: schema‑validated manifests, strict Pydantic models, guardrails in ROI calc.

2) Current Data Flow
--------------------

**Manifests**

- Generated via async CoinGecko manifest generator:
  - `v5/manifest_generator_async.py` builds:
    - `config/v5/tokens.json`
    - `config/v5/pools.json`
- Validated against JSON schemas in `v5/common/schemas/`:
  - `token_manifest.schema.json`
  - `pool_manifest.schema.json`
- `v5/common/config.Settings.ensure_manifests()` drives generation when:
  - Files are missing and `V5_GEN_MANIFEST=1` or `V5_REGEN_MANIFEST=1`.

**Ingest Path**

- RPC resync loop (in `IngestService._resync_loop`):
  - Uses `UniswapV3Poller` and `PancakeV3Poller` to poll `slot0` + `liquidity` for all pools.
  - Default `RESYNC_INTERVAL_SECONDS=600` (10 minutes); first resync runs immediately on start.
  - Emits `PoolState` snapshots into an internal queue.

- WebSocket adapters (`v5/ingest/ws_adapters.py`):
  - `UniswapV3WsAdapter` (ETH) and `PancakeV3WsAdapter` (BSC) connect to JSON‑RPC WS endpoints.
  - Subscribe via `eth_subscribe` logs filter:
    - Uniswap V3 swap topic: `SWAP_TOPIC_UNIV3`.
    - Pancake V3 swap topic: `SWAP_TOPIC_PANCAKE_V3`.
  - On new log:
    - Decode with `v5/ingest/event_decoder.decode_swap` → {pool, block_number, sqrtPriceX96, liquidity, tick}.
    - Wrap into `WsEvent` and enqueue for `WsRunner`.

- WS runner (`v5/ingest/ws_runner.py`):
  - Receives `WsEvent` objects from adapters.
  - Per‑pool buffer (`PoolBuffer`) maintains pending events.
  - `_state_from_event` converts each event directly into `PoolState`:
    - Sets `sqrt_price_x96`, `tick`, `liquidity`, `block_number`.
    - Sets `timestamp_ms` and `last_ws_refresh_ms`.
    - Marks `stream_ready=True`.
  - Enqueues resulting `PoolState` to ingest’s `out_queue`.

- Drain + stores (`v5/ingest/ingest_service.py`):
  - `_drain_out_queue`:
    - Writes each `PoolState` to hot cache (`HotCache.write_pool_state`).
      - Keys: `v5:hot:pool:<address>` (lowercased).
    - Writes to snapshot store (`SnapshotStore.write_if_changed`) keyed similarly.
    - Invokes `state_update_callbacks` (used by `RealtimeArbDetector`).

**State Store**

- Hot cache (`v5/state_store/hot_cache.py`):
  - Async Redis client.
  - Key space: `v5:hot:pool:<addr>` for pool states.
  - Batch reads/writes, TTL support, health check.

- Snapshot store (`v5/state_store/snapshot_store.py`):
  - Versioned snapshots keyed under `v5:snap:pool:<addr>`.
  - Idempotent write‑if‑changed behavior.
  - Metadata: creation time, last update, version, block number.

- Unified reader (`v5/state_store/state_reader.py`):
  - `get_pool_state(pool_address)`: hot first, then snapshot.
  - `get_all_pool_states(pools)`: merges hot + snapshot for a list.
  - `list_all_pool_addresses()`: scans hot cache keys to discover known pools.

3) Simulator & Detection
------------------------

**Graph (`v5/simulator/graph.py`)**

- `TokenGraph` builds a directed multigraph:
  - Nodes: token symbols (e.g., WETH, USDC, USDT).
  - Edges: `Edge` objects per pool, with:
    - `base`/`quote` tokens.
    - Associated `PoolMeta` (address, platform, fee tier, network).
  - Maintains multiple edges per token pair to represent different fee tiers (e.g., WETH/USDC 0.01%, 0.05%, 0.3%).

- Cycle generation:
  - Starts from unordered triangles (A,B,C).
  - Enumerates directed variants:
    - All 6 token permutations (rotations and direction).
    - For each permutation, multiplies combinations by fee tiers per edge.
  - Result: `cycle_variants` (currently 366) used by simulator.

**ROI & math (`v5/simulator/roi_calc.py`, `v5/simulator/uniswap_v3_math.py`)**

- `simulate_swap` in `uniswap_v3_math`:
  - Implements Uniswap V3 swap mechanics with Q96 fixed‑point math.
  - Handles tick movement within a single range; multi‑tick walk is partially implemented but still needs deeper validation.

- `compute_roi` (current behavior):
  - Walks through `OpportunityLeg` list:
    - For each leg, fetches `PoolState` from `pool_states`.
    - Runs `simulate_swap` with correct direction (`zero_for_one`) and fee tier.
    - Updates `amount` to the leg’s output.
    - Estimates per‑leg price impact as:
      - `impact = |price_after - price_before| / price_before`.
    - Sets `leg.price_impact`.
  - Computes cycle ROI:
    - `roi = (amount - initial_amount) / initial_amount`.
  - Guardrails (currently loosened for debugging):
    - `MAX_REALISTIC_ROI` and `MIN_REALISTIC_ROI` are wide (`±1000`) to expose raw behavior.
    - Still rejects completely absurd values in some places (logging “Unrealistic ROI … high/low, skipping”).
  - Applies strategy threshold:
    - Drops opportunities with `roi < min_roi`.

**ScenarioRunner (`v5/simulator/scenario_runner.py`)**

- Initialization:
  - Loads tokens/pools and builds `TokenGraph`.
  - Constructs `cycle_variants` (triangles × directions × fee tiers).
  - Tracks `state_reader`, `tokens`, `pools`, and `initial_amount`.

- `run_once()`:
  - Fetches all pool states for pools in the manifest via `StateReader`.
  - For each cycle variant:
    - Validates cycle via `_validate_cycle`:
      - Missing state → `invalid_state`.
      - Optional freshness check (`enforce_freshness` flag).
      - Zero or invalid sqrt price or liquidity → `invalid_state`.
    - Builds legs and calls `compute_roi`.
    - Records metrics per cycle (ROI histogram, simulation duration).
  - Sorts opportunities by ROI descending and returns list.

**RealtimeArbDetector (`v5/simulator/realtime_detector.py`)**

- Responsibilities:
  - Continuous monitoring loop (timer + WS‑triggered).
  - CSV logging of all detected opportunities per run.
  - Health logging (fresh pools, WS trigger counts, average calc times).
  - Metric updates for triggers and durations.

- Behavior:
  - On start:
    - Opens `run_logs/opportunities_<epoch>.csv` and writes header row.
    - Builds pool→cycle mapping from `scenario_runner.cycle_variants`.
  - Detection loop:
    - Timer loop: calls `_run_once(reason="timer")` on a fixed interval (`check_interval_ms`).
    - WS callback: `on_state_update` is invoked by `IngestService` when states change; triggers `_run_once(reason="ws")`.
  - `_run_once()`:
    - Calls `scenario_runner.run_once()`.
    - Filters opportunities with `total_roi >= min_roi`.
    - For each opportunity:
      - Appends to in‑memory deque (`recent`).
      - Publishes to metrics via `visibility.metrics_exporter`.
      - Logs to CSV:
        - `ts_ms`, `ts_iso`, `cycle_name`, `roi`, `profit`, `leg_count`, and per‑leg details:
          - `token_in->token_out@pool|fee=...|impact=...`.
    - Records timing metrics and logs `RUN_RESULT` or `WS_TRIGGER`.
  - Health loop:
    - Every 60s, logs:
      - Number of fresh pools (`pools_fresh`).
      - WS triggers and opportunities in the last minute.
      - Average calc duration.

4) Observability & Tools
------------------------

**Prometheus metrics (`v5/common/metrics.py`)**

- Pool metrics:
  - Prices, ticks, liquidity, last snapshot/WS times, stale flags.
- Ingest:
  - Snapshot counts, WS event counts, errors, WS backpressure gauge.
- Simulation:
  - ROI distribution (`CYCLE_ROI_HISTOGRAM`).
  - Cycle simulation durations.
  - Detector triggers and opportunities.
- WS:
  - Reconnect counters.
  - Last message timestamps.

**observe.py (`v5/tools/observe.py`)**

- Purpose:
  - End‑to‑end smoke test of V5 pipeline over a given duration.
  - Provides a concise summary of data freshness and opportunity activity.
  - Drives `IngestService` + `ScenarioRunner` + `RealtimeArbDetector`.

- Output:
  - Console:
    - Freshness buckets:
      - `Fresh pools <=30s`, `<=90s`, `<=300s`.
    - Age distributions:
      - Overall timestamp age (any of `last_ws_refresh_ms`, `last_snapshot_ms`, `timestamp_ms`).
      - WS age distribution.
      - Snapshot age distribution.
    - Recent opportunities count (capped by in‑memory deque, currently 50).
  - CSV:
    - One CSV per run: `run_logs/opportunities_<epoch>.csv`.
    - Contains all opportunities detected during the run (not just the “recent” tail).

**ws_probe.py (`v5/tools/ws_probe.py`)**

- Minimal tool to verify WS logs arrive for specific pools:
  - Connects to a WS URL.
  - Subscribes to Swap logs for listed pool addresses.
  - Decodes swaps with `decode_swap` and prints a small sample.
  - Times out after N seconds of inactivity.

**diagnose_pipeline.py (`v5/tools/diagnose_pipeline.py`)**

- Stepwise diagnostics:
  - Test 1: Manifest loading (tokens/pools count).
  - Test 2: Redis connectivity (hot + snapshot).
  - Test 3: RPC single call (blockNumber).
  - Test 4: Poll single pool (slot0/liquidity shape).
  - Test 5: Write/read to stores.
  - Test 6: Mini ingest loop, fresh pool count.
  - Test 7: WS connectivity (short connect test).

5) How to Run V5 (DEX Only)
---------------------------

**Environment example (QuickNode)**:

```bash
export ETHEREUM_RPC_URL="https://polished-clean-market.quiknode.pro/<key>"
export ETHEREUM_WS_URL="wss://polished-clean-market.quiknode.pro/<key>"
export BSC_RPC_URL="https://polished-clean-market.bsc.quiknode.pro/<key>"
export BSC_WS_URL="wss://polished-clean-market.bsc.quiknode.pro/<key>"
export REDIS_URL="redis://localhost:6379/0"
export RESYNC_INTERVAL_SECONDS=600   # 10m
export STRATEGY_TRI_THRESHOLD_BPS=10 # 0.10% ROI threshold
```

**Commands**:

- Observe run (e.g. 120 seconds):

```bash
PYTHONPATH=. LOG_LEVEL=INFO python -m v5.tools.observe 120
```

- WebSocket probe (ETH, USDC/WETH 0.3%):

```bash
PYTHONPATH=. python -m v5.tools.ws_probe \
  --ws "$ETHEREUM_WS_URL" \
  --network eth \
  --pool 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 \
  --timeout 30
```

- Pipeline diagnostics:

```bash
PYTHONPATH=. python -m v5.tools.diagnose_pipeline
```

6) Current State & Open Issues (as of latest work)
-------------------------------------------------

Working well:

- Manifests:
  - Tokens/pools load correctly (recent runs: 97 tokens, 120 pools).
  - JSON schemas catch malformed entries early (e.g., stray `chain` fields).
- Ingest:
  - RPC polling and WS subscriptions both active.
  - WS decode works for Uniswap V3 and Pancake V3 topics.
  - Hot cache/snapshot store receive states; `last_ws_refresh_ms` and `stream_ready` set.
- Freshness:
  - In recent 60–120s runs:
    - ~62/120 pools fresh ≤30s.
    - All 120 pools fresh ≤90s/300s.
  - WS age p50 ~4–8s, p95 ~16–60s.
- Simulation:
  - Cycle variants generation (366) correctly handles multiple fee tiers and directions.
  - Realtime detector runs, logs health and opportunities.
  - CSV logging provides clean view of ROI, profit, and per‑leg impacts.

Known issues / TODO:

1. Guardrails & math validation
   - Guardrails have been loosened to inspect raw opportunities (ROI caps ±1000, high price‑impact thresholds).
   - CSVs show many unrealistic ROIs (indicating either bad pool states or math edge cases).
   - TODO:
     - Inspect CSV output for realistic cycles and set appropriate caps:
       - Example: MAX_REALISTIC_ROI ~ 0.2, MIN_REALISTIC_ROI ~ -0.1.
       - Price impact cap ~ 0.2 per leg.
     - Add tests comparing `simulate_swap` against known on‑chain test vectors (tick‑by‑tick).

2. Invalid pool states
   - Many cycles are skipped with `invalid_state` due to:
     - Missing pool state in Redis.
     - Zero/garbage `sqrt_price_x96` or `liquidity`.
   - TODO:
     - Add sampler logging of which pool addresses and states cause `invalid_state`.
     - Optionally drop “toxic” pools from cycle generation for production runs.

3. Metrics server / dashboard
   - DashboardServer exists but isn’t wired via uvicorn in `v5/main.py` as a long‑running HTTP service.
   - TODO:
     - Provide a dedicated `metrics_server` entrypoint that:
       - Starts FastAPI HTTP server for `/metrics`, `/health`, `/pools`, `/opportunities`, `/ws/status`, `/cycles`, `/data-quality`.

4. Deep Uniswap V3 math
   - Current implementation accurately handles basic swaps but:
     - Multi‑tick walking with per‑tick liquidity changes is only partially validated.
   - TODO:
     - Add full TickMath port with binary‑search inverse.
     - Validate round‑trip tick ↔ sqrtPrice conversions for deep ticks.
     - Replay real slot0 + tick vector fixtures for integration tests.

5. Integration tests
   - V5 has good unit and module tests, but limited full end‑to‑end integration tests.
   - TODO:
     - Add integration test harness that:
       - Seeds Redis with synthetic states.
       - Runs a short ingest+simulator loop.
       - Asserts opportunities and metrics behave as expected.

7) Relationship to Legacy CEX Stack
-----------------------------------

- V5 is intentionally decoupled from the legacy CEX stack:
  - No direct CCXT usage; all CEX ingestion lives under `python-ingest/`.
  - Java CEX stack remains unchanged and still builds (`mvn test` passes).
- V5 can optionally correlate with CEX prices in the future via shared manifests and state abstractions, but that is out of scope for the current implementation.

8) When You Return to This Project
----------------------------------

If you’re picking this up after a break:

1. Run diagnostics:
   - `PYTHONPATH=. python -m v5.tools.diagnose_pipeline`
     - Confirm manifests, Redis, RPC, and WS all look healthy.
2. Run a short observation:
   - `PYTHONPATH=. LOG_LEVEL=INFO python -m v5.tools.observe 120`
   - Check:
     - Freshness buckets.
     - Age distributions.
     - Presence of opportunities (and inspect `run_logs/opportunities_<ts>.csv`).
3. Decide next focus:
   - If opportunities look realistic: tighten guardrails and consider adding executor logic.
   - If ROIs look absurd: prioritize math validation and invalid‑state handling.
   - If freshness is poor: debug WS/RPC endpoints or resync intervals.

This README is meant to serve as a high‑signal map of what V5 does today, how it’s wired, and what remains to be implemented or validated before using it for serious dry‑run or live trading experiments.

