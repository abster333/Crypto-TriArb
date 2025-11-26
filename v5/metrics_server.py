"""Standalone metrics/dashboard server for V5."""

from __future__ import annotations

import asyncio
import argparse
import logging

import uvicorn

from v5.common.config import Settings
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader
from v5.simulator.scenario_runner import ScenarioRunner
from v5.simulator.realtime_detector import RealtimeArbDetector
from v5.visibility.dashboard_server import DashboardServer
from v5.state_store.manifest_validator import validate_manifests

log = logging.getLogger(__name__)


async def build_app(settings: Settings):
    await settings.ensure_manifests(settings.generate_manifests)
    validate_manifests(settings.token_manifest_path, settings.pool_manifest_path)
    hot = HotCache(settings.redis_url)
    snap = SnapshotStore(settings.redis_url)
    await hot.start(); await snap.start()
    reader = StateReader(hot, snap)
    tokens = settings.load_tokens()
    pools = settings.load_pools()
    simulator = ScenarioRunner(reader, tokens, pools)
    detector = RealtimeArbDetector(simulator)
    server = DashboardServer(reader, detector)
    return server, hot, snap


async def main():
    parser = argparse.ArgumentParser(description="Run metrics/dashboard server only")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9100)
    args = parser.parse_args()

    settings = Settings()
    logging.basicConfig(level=logging.INFO)

    server, hot, snap = await build_app(settings)
    app = await server.start()
    config = uvicorn.Config(app, host=args.host, port=args.port, log_level="info")
    uvicorn_server = uvicorn.Server(config)
    log.info("Serving metrics/dashboard on http://%s:%s", args.host, args.port)
    try:
        await uvicorn_server.serve()
    finally:
        await hot.close()
        await snap.close()


if __name__ == "__main__":
    asyncio.run(main())
