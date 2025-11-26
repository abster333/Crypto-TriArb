from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Dict

try:
    from aiohttp import web
except ImportError:  # pragma: no cover
    web = None

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # type: ignore
except Exception:  # pragma: no cover
    generate_latest = None
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"

log = logging.getLogger(__name__)


async def start_health_server(port: int, state_supplier: Callable[[], Dict]) -> asyncio.AbstractServer | None:
    """Start a tiny aiohttp server exposing /healthz and /metrics. Returns the runner (or None if deps missing)."""
    if web is None:
        log.warning("aiohttp not installed; health server disabled")
        return None

    async def healthz(_request: web.Request) -> web.Response:  # type: ignore
        data = state_supplier() or {}
        return web.json_response({"status": "ok", **data})

    async def metrics(_request: web.Request) -> web.Response:  # type: ignore
        if generate_latest is None:
            return web.Response(status=503, text="prometheus_client not installed")
        payload = generate_latest()
        return web.Response(body=payload, headers={"Content-Type": CONTENT_TYPE_LATEST})

    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/metrics", metrics)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Health server listening on :%s", port)
    return runner
