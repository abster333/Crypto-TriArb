"""Generic pool poller interface and Uniswap/Pancake adapters."""

from __future__ import annotations

import abc
import time
from collections import defaultdict
from typing import Iterable, List, Dict

from v5.common.models import PoolMeta, PoolState
from v5.ingest.rpc_server import BatchRpcServer
from v5.common import metrics
from v5.simulator.realtime_detector import price_from_sqrt


def decode_slot0(slot0_raw: str) -> tuple[int, int]:
    """Decode slot0 raw hex into (sqrtPriceX96, tick)."""
    if not slot0_raw or not slot0_raw.startswith("0x"):
        return 0, 0
    data = slot0_raw[2:]
    if len(data) < 128:
        return 0, 0
    sqrt_price_x96 = int(data[:64], 16)
    tick_raw = int(data[64:128], 16)
    if tick_raw >= 2**255:
        tick_raw -= 2**256
    return sqrt_price_x96, tick_raw


class PoolPoller(abc.ABC):
    """Abstract poller emitting PoolState objects."""

    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def stop(self) -> None: ...

    @abc.abstractmethod
    async def poll_once(self) -> List[PoolState]: ...


class UniswapV3Poller(PoolPoller):
    """Poll Uniswap V3-compatible pools via BatchRpcServer."""

    def __init__(self, rpc: BatchRpcServer, pools: Iterable[PoolMeta], poll_interval: float = 12.0) -> None:
        self.rpc = rpc
        self.pools = list(pools)
        self.poll_interval = max(5.0, poll_interval)
        self._running = False
        self._backoff_until: Dict[str, float] = defaultdict(float)

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def poll_once(self) -> List[PoolState]:
        now_ms = int(time.time() * 1000)
        results: List[PoolState] = []
        if not self.pools:
            return results
        pools_by_net: Dict[str, List[PoolMeta]] = defaultdict(list)
        for meta in self.pools:
            if time.time() < self._backoff_until[meta.pool_address]:
                continue
            pools_by_net[meta.network].append(meta)
        for network, metas in pools_by_net.items():
            for meta in metas:
                state = await self.rpc.submit_pool_state(meta.network, meta.pool_address)
                if not state:
                    self._backoff_until[meta.pool_address] = time.time() + 5
                    metrics.INGEST_ERRORS.labels(type="poll").inc()
                    continue
                slot0_raw, liq = state.get("slot0"), state.get("liquidity")
                if not slot0_raw or not liq:
                    continue
                sqrt_price_x96, tick = decode_slot0(slot0_raw)
                metrics.INGEST_RPC_CALLS.labels(network=meta.network, method="pool_state").inc()
                price = price_from_sqrt(sqrt_price_x96, meta.dec0, meta.dec1)
                results.append(
                    PoolState(
                        pool_meta=meta,
                        sqrt_price_x96=str(sqrt_price_x96),
                        tick=int(tick),
                        liquidity=str(liq),
                        liquidity_int=int(liq),
                        price=price,
                        block_number=state.get("block_number", 0) or 0,
                        timestamp_ms=now_ms,
                        last_snapshot_ms=now_ms,
                        stream_ready=False,
                    )
                )
        return results


class PancakeV3Poller(UniswapV3Poller):
    """Alias for BSC Pancake v3; separated for future custom logic."""

    ...


__all__ = ["PoolPoller", "UniswapV3Poller", "PancakeV3Poller", "decode_slot0"]
