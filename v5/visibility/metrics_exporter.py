"""Metrics export helpers."""

from __future__ import annotations

from v5.common import metrics
from v5.common.models import PoolState, Opportunity


def publish_pool_state(state: PoolState) -> None:
    addr = state.pool_meta.pool_address
    plat = state.pool_meta.platform
    net = state.pool_meta.network
    metrics.POOL_PRICE.labels(addr, plat, net).set(float(state.sqrt_price_x96) if state.sqrt_price_x96 else 0)
    metrics.POOL_TICK.labels(addr, plat, net).set(state.tick)
    metrics.POOL_LIQUIDITY.labels(addr, plat, net).set(float(state.liquidity))
    if state.last_snapshot_ms is not None:
        metrics.POOL_LAST_SNAPSHOT_MS.labels(addr, plat, net).set(state.last_snapshot_ms)
    if state.last_ws_refresh_ms is not None:
        metrics.POOL_LAST_WS_MS.labels(addr, plat, net).set(state.last_ws_refresh_ms)
    metrics.POOL_STALE.labels(addr, plat, net).set(0)


def publish_opportunity(opp: Opportunity) -> None:
    metrics.OPP_ROI.labels(cycle=str(opp.legs)).set(opp.total_roi)
    metrics.OPP_PROFIT_USD.labels(cycle=str(opp.legs)).set(opp.profit_usd or 0)


__all__ = ["publish_pool_state", "publish_opportunity"]
