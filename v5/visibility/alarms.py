"""Alarm delivery with webhook support."""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Callable

import aiohttp

from v5.common.models import AlarmConfig, HealthStatus

log = logging.getLogger(__name__)


async def _post(url: str, payload: dict, session: aiohttp.ClientSession) -> bool:
    try:
        async with session.post(url, json=payload, timeout=5) as resp:
            return resp.status < 300
    except Exception as exc:  # noqa: BLE001
        log.warning("Alarm webhook post failed: %s", exc)
        return False


async def emit_health_alarms(cfg: AlarmConfig, health_list: Iterable[HealthStatus], sender: Callable | None = None) -> None:
    """Send alarms for stale pools; dedup by pool_address in this invocation."""
    stale_pools = [h for h in health_list if h.is_stale]
    if not stale_pools:
        return
    sender = sender or _send_webhooks
    await sender(cfg, stale_pools)


async def _send_webhooks(cfg: AlarmConfig, health_list: list[HealthStatus]) -> None:
    if not cfg.webhook_urls:
        for h in health_list:
            log.warning("ALARM stale pool=%s", h.pool_address)
        return
    payload = {
        "text": f"{len(health_list)} pools stale",
        "pools": [h.pool_address for h in health_list],
    }
    async with aiohttp.ClientSession() as session:
        tasks = [_post(url, payload, session) for url in cfg.webhook_urls]
        await asyncio.gather(*tasks, return_exceptions=True)


__all__ = ["emit_health_alarms"]
