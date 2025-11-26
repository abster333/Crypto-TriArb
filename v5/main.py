"""Entry point for V5 service pipeline with optional dashboard server."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
from typing import Optional

import uvicorn

from v5.common.config import Settings
from v5.ingest.ingest_service import IngestService
from v5.ingest.rpc_server import BatchRpcServer
from v5.simulator.realtime_detector import RealtimeArbDetector
from v5.simulator.scenario_runner import ScenarioRunner
from v5.state_store.hot_cache import HotCache
from v5.state_store.manifest_validator import validate_manifests
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader
from v5.visibility.dashboard_server import DashboardServer

log = logging.getLogger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def build_components(settings: Settings):
    """Initialize core components; caller decides what to start."""
    # Only regenerate manifests if explicitly enabled; otherwise trust existing files.
    if settings.generate_manifests or os.environ.get("V5_REGEN_MANIFEST") == "1":
        await settings.ensure_manifests(settings.generate_manifests)
    validate_manifests(settings.token_manifest_path, settings.pool_manifest_path)

    hot_cache = HotCache(settings.redis_url)
    snapshots = SnapshotStore(settings.redis_url)
    rpc = BatchRpcServer()

    await hot_cache.start()
    await hot_cache.clear_all()
    await snapshots.start()
    await snapshots.clear_all()

    pools = settings.load_pools()
    tokens = settings.load_tokens()
    state_reader = StateReader(hot_cache, snapshots)
    from decimal import Decimal

    simulator = ScenarioRunner(
        state_reader,
        tokens,
        pools,
        initial_amount=Decimal(str(settings.initial_amount)),
        min_roi_threshold=settings.strategy_tri_threshold_bps / 10_000.0,
        enforce_freshness=True,
    )
    detector = RealtimeArbDetector(simulator, min_roi_threshold=settings.strategy_tri_threshold_bps / 10_000.0)
    ingest = IngestService(
        rpc,
        pools,
        hot_cache,
        snapshots,
        uniswap_ws_url=settings.ethereum_ws_url or settings.alchemy_ws_url or "",
        pancake_ws_url=settings.bsc_ws_url or "",
        resync_interval_seconds=settings.resync_interval_seconds,
        enable_poll_fallback=settings.enable_poll_fallback,
        ws_sub_batch_size=settings.ws_sub_batch_size,
    )
    dashboard = DashboardServer(state_reader, detector)
    ingest.state_update_callbacks.append(detector.on_state_update)
    return {
        "settings": settings,
        "hot_cache": hot_cache,
        "snapshots": snapshots,
        "rpc": rpc,
        "pools": pools,
        "tokens": tokens,
        "state_reader": state_reader,
        "simulator": simulator,
        "detector": detector,
        "ingest": ingest,
        "dashboard": dashboard,
    }


async def serve_dashboard(app, host: str, port: int) -> None:
    """Start uvicorn server for the dashboard/metrics app."""
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def run(args: Optional[list[str]] = None) -> None:
    settings = Settings()
    _configure_logging(settings.log_level)

    parser = argparse.ArgumentParser(description="Run V5 service")
    parser.add_argument("--dashboard-only", action="store_true", help="Start dashboard server without ingest/simulator loops")
    parser.add_argument("--no-dashboard", action="store_true", help="Do not start uvicorn dashboard server")
    parser.add_argument("--host", default="0.0.0.0", help="Dashboard host (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=settings.metrics_port, help=f"Dashboard port (default {settings.metrics_port})")
    parsed = parser.parse_args(args)

    comps = await build_components(settings)

    tasks: list[asyncio.Task] = []
    # Start ingest/simulator unless dashboard-only
    if not parsed.dashboard_only:
        await comps["rpc"].start()
        await comps["ingest"].start()
        await comps["detector"].start()
        log.info("Ingest and detector started")

    # Start dashboard server unless disabled
    if not parsed.no_dashboard:
        app = await comps["dashboard"].start()
        tasks.append(asyncio.create_task(serve_dashboard(app, parsed.host, parsed.port), name="uvicorn-dashboard"))
        log.info("Dashboard serving on http://%s:%s", parsed.host, parsed.port)

    log.info("V5 service started")
    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        for t in tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        # stop ingest only if it was started
        with contextlib.suppress(Exception):
            await comps["ingest"].stop()
        with contextlib.suppress(Exception):
            await comps["rpc"].stop()
        with contextlib.suppress(Exception):
            await comps["hot_cache"].close()
        with contextlib.suppress(Exception):
            await comps["snapshots"].close()


if __name__ == "__main__":
    asyncio.run(run())
