"""Minimal WS probe to verify swap logs arrive.

Usage:
  PYTHONPATH=. python -m v5.tools.ws_probe \
    --ws wss://ethereum-rpc.publicnode.com \
    --network eth \
    --pool 0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8  # USDC/WETH 0.3%

Notes:
  - Subscribes to logs for the provided pool addresses.
  - Prints the first few messages and counts decoded swaps.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from typing import List

import websockets

from v5.ingest.event_decoder import decode_swap, SWAP_TOPIC_UNIV3, SWAP_TOPIC_PANCAKE_V3


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ws", required=True, help="WebSocket URL")
    parser.add_argument("--network", default="eth", help="Network label")
    parser.add_argument("--pool", action="append", required=True, help="Pool address (can repeat)")
    parser.add_argument("--max", type=int, default=10, help="Number of decoded swaps to print")
    parser.add_argument("--timeout", type=int, default=15, help="Seconds to wait with no messages before exiting")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    pools = [p.lower() for p in args.pool]
    # Shorter keepalive to avoid server-side idle timeouts
    async with websockets.connect(args.ws, ping_interval=10, ping_timeout=10) as ws:
        sub = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": pools,
                    "topics": [],
                },
            ],
        }
        await ws.send(json.dumps(sub))
        logging.info("sent subscription for %d pools", len(pools))
        ack = await ws.recv()
        logging.info("sub ack: %s", ack)
        decoded = 0
        received = 0
        ws_timeout = args.timeout
        ws_deadline = asyncio.get_event_loop().time() + ws_timeout
        while decoded < args.max:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, ws_deadline - asyncio.get_event_loop().time()))
            except asyncio.TimeoutError:
                logging.warning("timeout with no messages for %ss", args.timeout)
                break
            received += 1
            ws_deadline = asyncio.get_event_loop().time() + ws_timeout
            try:
                data = json.loads(msg)
            except Exception:
                logging.debug("non-json message: %s", msg)
                continue
            params = (data or {}).get("params", {})
            result = params.get("result") or {}
            swap = decode_swap(result)
            if swap:
                decoded += 1
                logging.info(
                    "swap %d: pool=%s block=%s sqrt=%s liq=%s tick=%s",
                    decoded,
                    swap["pool"][:10],
                    swap["block_number"],
                    swap["sqrt_price_x96"],
                    swap["liquidity"],
                    swap["tick"],
                )
            else:
                if "error" in data:
                    logging.warning("ws error: %s", data["error"])
                elif "result" in data:
                    logging.debug("ws msg: %s", data)
        logging.info("done: received=%d decoded_swaps=%d", received, decoded)


if __name__ == "__main__":
    asyncio.run(main())
