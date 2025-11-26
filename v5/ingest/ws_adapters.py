"""WebSocket adapters for Uniswap v3 / Pancake v3."""

from __future__ import annotations

import abc
import asyncio
import json
import logging
import random
from typing import List, Optional

import websockets

from v5.ingest.event_decoder import decode_swap, SWAP_TOPIC_UNIV3, SWAP_TOPIC_PANCAKE_V3
from v5.ingest.ws_types import WsEvent
from v5.ingest.circuit_breaker import CircuitBreaker
from v5.common import metrics

log = logging.getLogger(__name__)


class WsAdapter(abc.ABC):
    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def subscribe(self, pools: List[str]) -> None: ...

    @abc.abstractmethod
    async def recv_event(self) -> Optional[WsEvent]: ...

    @abc.abstractmethod
    async def close(self) -> None: ...


class _BaseJsonRpcWs(WsAdapter):
    def __init__(self, ws_url: str, network: str, breaker: Optional[CircuitBreaker] = None) -> None:
        self.ws_url = ws_url
        self.network = network
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._id = 0
        self._breaker = breaker or CircuitBreaker(failure_threshold=3, reset_timeout=15)

    async def connect(self) -> None:
        backoff = 1
        while True:
            try:
                try:
                    async def _do_connect():
                        return await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
                    self._ws = await self._breaker.call(_do_connect)
                except TypeError:
                    self._ws = await self._breaker.call(lambda: websockets.connect(self.ws_url))
            except websockets.InvalidStatusCode as exc:
                log.error("WS handshake rejected url=%s network=%s status=%s", self.ws_url, self.network, exc.status_code)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            except websockets.InvalidURI as exc:
                log.error("WS invalid URI url=%s network=%s err=%s", self.ws_url, self.network, exc)
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("WS connect failed url=%s network=%s err=%s; retrying in %ss", self.ws_url, self.network, exc, backoff)
                jitter = random.uniform(0.5, 1.5)
                await asyncio.sleep(backoff * jitter)
                backoff = min(backoff * 2, 30)
                continue
            try:
                metrics.WS_RECONNECTS.labels(network=self.network).inc()
            except Exception:
                pass
            log.info("WS_CONNECTED url=%s network=%s", self.ws_url, self.network)
            return

    async def _send(self, payload: dict) -> None:
        assert self._ws
        log.debug("WS_SEND network=%s payload=%s", self.network, payload)
        await self._ws.send(json.dumps(payload))

    async def recv_event(self) -> Optional[WsEvent]:
        assert self._ws
        try:
            msg = await self._ws.recv()
        except websockets.ConnectionClosed as exc:
            log.warning("WS_CLOSED network=%s code=%s reason=%s", self.network, exc.code, exc.reason)
            await self.connect()
            return None
        except Exception as exc:
            log.warning("WS_RECV_ERROR network=%s err=%s", self.network, exc)
            await self.connect()
            return None
        log.debug("WS_RAW network=%s type=%s len=%s", self.network, type(msg), len(msg) if hasattr(msg, "__len__") else "na")
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = msg.decode()
            except Exception:
                log.debug("WS_DECODE_FAIL network=%s reason=bytes", self.network)
                return None
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            log.debug("WS_DECODE_FAIL network=%s reason=json", self.network)
            return None
        params = data.get("params", {})
        log_obj = params.get("result") or {}
        decoded = decode_swap(log_obj)
        if not decoded:
            if "error" in data:
                log.warning("WS_ERROR network=%s err=%s", self.network, data.get("error"))
            return None
        try:
            import time
            from v5.common import metrics

            metrics.WS_LAST_MESSAGE_MS.labels(self.network).set(time.time() * 1000)
        except Exception:
            pass
        import time
        log.info(
            "WS_DECODE network=%s pool=%s block=%s sqrt=%s liq=%s tick=%s",
            self.network,
            decoded["pool"][:8],
            decoded["block_number"],
            decoded["sqrt_price_x96"],
            decoded["liquidity"],
            decoded["tick"],
        )
        return WsEvent(
            pool=decoded["pool"],
            block_number=decoded["block_number"],
            ts_ms=int(time.time() * 1000),
            sqrt_price_x96=decoded["sqrt_price_x96"],
            liquidity=decoded["liquidity"],
            tick=decoded["tick"],
        )

    async def close(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
                log.info("WS_CLOSED network=%s", self.network)
            except Exception as exc:  # noqa: BLE001
                log.debug("WS_CLOSE_ERROR network=%s err=%s", self.network, exc)
            finally:
                self._ws = None


class UniswapV3WsAdapter(_BaseJsonRpcWs):
    """Ethereum mainnet WS."""

    def __init__(self, ws_url: str):
        super().__init__(ws_url, "eth")

    async def subscribe(self, pools: List[str]) -> None:
        if not pools:
            return
        self._id += 1
        topics = [[SWAP_TOPIC_UNIV3]]
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": self._id,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [p.lower() for p in pools],
                        "topics": topics,
                    },
                ],
            }
        )
        log.info("WS_SUBSCRIBED network=%s pools=%d", self.network, len(pools))


class PancakeV3WsAdapter(_BaseJsonRpcWs):
    """BSC WS adapter."""

    def __init__(self, ws_url: str):
        super().__init__(ws_url, "bsc")

    async def subscribe(self, pools: List[str]) -> None:
        if not pools:
            return
        self._id += 1
        topics = [[SWAP_TOPIC_PANCAKE_V3]]
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": self._id,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [p.lower() for p in pools],
                        "topics": topics,
                    },
                ],
            }
        )
        log.info("WS_SUBSCRIBED network=%s pools=%d", self.network, len(pools))


__all__ = ["WsAdapter", "UniswapV3WsAdapter", "PancakeV3WsAdapter"]
