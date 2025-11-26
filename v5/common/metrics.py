"""Prometheus metrics helpers for V5."""

from __future__ import annotations

from typing import Dict

from prometheus_client import Counter, Gauge, Histogram

# Gauges
POOL_PRICE = Gauge("dex_pool_price", "Current mid price", ["pool_address", "platform", "network"])
POOL_TICK = Gauge("dex_pool_tick", "Current tick", ["pool_address", "platform", "network"])
POOL_LIQUIDITY = Gauge("dex_pool_liquidity", "Current liquidity", ["pool_address", "platform", "network"])
POOL_LAST_SNAPSHOT_MS = Gauge("dex_pool_last_snapshot_ms", "Last snapshot timestamp (ms)", ["pool_address", "platform", "network"])
POOL_LAST_WS_MS = Gauge("dex_pool_last_ws_ms", "Last websocket update (ms)", ["pool_address", "platform", "network"])
POOL_STALE = Gauge("dex_pool_stale", "1 if pool data is stale", ["pool_address", "platform", "network"])
POOL_STALE_SNAPSHOT = Gauge("dex_pool_stale_snapshot", "1 if snapshot stale", ["pool_address", "platform", "network"])
POOL_STALE_STREAM = Gauge("dex_pool_stale_stream", "1 if stream stale", ["pool_address", "platform", "network"])

# Counters
INGEST_SNAPSHOTS = Counter("dex_ingest_snapshots_total", "Total ingested snapshots", ["platform", "network"])
INGEST_RPC_CALLS = Counter("dex_ingest_rpc_calls_total", "RPC calls", ["network", "method"])
INGEST_WS_EVENTS = Counter("dex_ingest_ws_events_total", "WS events received", ["platform", "network"])
INGEST_ERRORS = Counter("dex_ingest_errors_total", "Ingest errors", ["type"])
SIM_OPPORTUNITIES = Counter("dex_simulator_opportunities_total", "Detected opportunities", ["cycle"])
OPP_ROI = Gauge("dex_opportunity_roi", "ROI for opportunity", ["cycle"])
OPP_PROFIT_USD = Gauge("dex_opportunity_profit_usd", "Profit USD", ["cycle"])
INGEST_WS_BACKPRESSURE = Gauge("dex_ingest_ws_queue_depth", "WS queue depth", ["network"])
ACTIVE_CYCLES = Gauge("dex_active_cycles", "Number of cycles being monitored")
WS_LAST_MESSAGE_MS = Gauge("dex_ws_last_message_ms", "Timestamp of last WS message", ["network"])
CIRCUIT_STATE = Gauge("dex_circuit_state", "Circuit breaker state (0=closed,1=half_open,2=open)", ["component"])
RPC_LATENCY_SECONDS = Histogram("dex_rpc_latency_seconds", "RPC call latency", ["network", "method"], buckets=(0.01,0.05,0.1,0.2,0.5,1,2))
WS_RECONNECTS = Counter("dex_ws_reconnects_total", "Websocket reconnect attempts", ["network"])
DETECTOR_TRIGGERS = Counter("dex_detector_triggers_total", "Detector triggers by reason", ["reason"])
CYCLE_SKIP = Counter("dex_cycle_skipped_total", "Cycles skipped with reason", ["cycle", "reason"])
WS_TO_SIM_SECONDS = Histogram("dex_ws_to_sim_seconds", "Latency from WS update to simulation trigger", buckets=(0.005,0.01,0.02,0.05,0.1,0.2,0.5,1))
CYCLE_CALC_SECONDS = Histogram("dex_cycle_calc_seconds", "Cycle calculation duration", ["cycle"], buckets=(0.0005,0.001,0.002,0.005,0.01,0.02,0.05,0.1))

# Histograms
ROI_DISTRIBUTION = Histogram("dex_roi_distribution", "ROI histogram", buckets=( -0.1, 0, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1))
SIM_DURATION_SECONDS = Histogram("dex_simulation_duration_seconds", "Simulation loop duration seconds", buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2))
CYCLE_SIMULATION_TIME = Histogram("dex_cycle_simulation_seconds", "Time to simulate a cycle", ["cycle"], buckets=(0.001, 0.005, 0.01, 0.05, 0.1))
CYCLE_ROI_HISTOGRAM = Histogram("dex_cycle_roi", "ROI distribution per cycle", ["cycle"], buckets=(-0.01, -0.001, 0, 0.0001, 0.001, 0.01, 0.1))

# State store metrics
STATE_CACHE_HIT = Counter("dex_state_cache_hit_total", "Hot cache hits", ["source"])
STATE_CACHE_MISS = Counter("dex_state_cache_miss_total", "Hot cache misses", ["source"])
STATE_READ_ERRORS = Counter("dex_state_read_errors_total", "Errors reading state", ["source"])
STATE_WRITE_ERRORS = Counter("dex_state_write_errors_total", "Errors writing state", ["source"])
SNAPSHOT_VERSION = Gauge("dex_snapshot_version", "Snapshot version per pool", ["pool_address"])


def update_pool_metrics(pool_address: str, platform: str, network: str, *, price: float, tick: int, liquidity: float,
                        last_snapshot_ms: int | None, last_ws_ms: int | None, stale: bool) -> None:
    """Set all per-pool gauges in one call."""
    POOL_PRICE.labels(pool_address, platform, network).set(price)
    POOL_TICK.labels(pool_address, platform, network).set(tick)
    POOL_LIQUIDITY.labels(pool_address, platform, network).set(liquidity)
    if last_snapshot_ms is not None:
        POOL_LAST_SNAPSHOT_MS.labels(pool_address, platform, network).set(last_snapshot_ms)
    if last_ws_ms is not None:
        POOL_LAST_WS_MS.labels(pool_address, platform, network).set(last_ws_ms)
    POOL_STALE.labels(pool_address, platform, network).set(1 if stale else 0)


def increment_counter(counter: Counter, labels: Dict[str, str]) -> None:
    """Increment a labeled counter safely."""
    counter.labels(**labels).inc()


def observe_histogram(hist: Histogram, value: float) -> None:
    """Record a value in a histogram."""
    hist.observe(value)


__all__ = [
    "POOL_PRICE",
    "POOL_TICK",
    "POOL_LIQUIDITY",
    "POOL_LAST_SNAPSHOT_MS",
    "POOL_LAST_WS_MS",
    "POOL_STALE",
    "POOL_STALE_SNAPSHOT",
    "POOL_STALE_STREAM",
    "INGEST_SNAPSHOTS",
    "INGEST_WS_EVENTS",
    "INGEST_ERRORS",
    "SIM_OPPORTUNITIES",
    "ACTIVE_CYCLES",
    "WS_LAST_MESSAGE_MS",
    "CIRCUIT_STATE",
    "RPC_LATENCY_SECONDS",
    "WS_RECONNECTS",
    "ROI_DISTRIBUTION",
    "SIM_DURATION_SECONDS",
    "CYCLE_SIMULATION_TIME",
    "CYCLE_ROI_HISTOGRAM",
    "update_pool_metrics",
    "increment_counter",
    "observe_histogram",
]
