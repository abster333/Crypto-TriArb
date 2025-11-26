"""
Compare L1 vs depth snapshots side-by-side from NATS.

Usage:
  PYTHONUNBUFFERED=1 \
  NATS_URL=nats://127.0.0.1:4222 \
  L1_SUBJECT=md.snapshot \
  DEPTH_SUBJECT=md.depth \
  python scripts/compare_feeds.py

It prints a periodic report of symbols seen in each feed and a few sample
mid prices per exchange/symbol so you can confirm both feeds are aligned.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict
from typing import Dict, Tuple

import nats


def norm_symbol(sym: str) -> str:
    return sym.replace("/", "").replace("-", "").replace("_", "").upper()


class FeedState:
    def __init__(self) -> None:
        self.data: Dict[Tuple[str, str], Tuple[float, float, int]] = {}

    def update(self, payload: dict) -> None:
        ex = payload.get("exchange")
        sym = norm_symbol(payload.get("symbol", ""))
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if not ex or not sym or not bids or not asks:
            return
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        ts = int(payload.get("ts_ingest", payload.get("ts_event", time.time() * 1000)))
        self.data[(ex, sym)] = (bid, ask, ts)

    def snapshot(self):
        return self.data.copy()


async def main() -> None:
    nats_url = os.getenv("NATS_URL") or os.getenv("DEPTH_NATS_URL", "nats://127.0.0.1:4222")
    l1_subject = os.getenv("L1_SUBJECT", "md.snapshot")
    depth_subject = os.getenv("DEPTH_SUBJECT", "md.depth")
    interval = float(os.getenv("REPORT_INTERVAL", "5"))

    l1 = FeedState()
    depth = FeedState()

    nc = await nats.connect(nats_url)

    async def handler_l1(msg):
        try:
            payload = json.loads(msg.data.decode())
            l1.update(payload)
        except Exception:
            pass

    async def handler_depth(msg):
        try:
            payload = json.loads(msg.data.decode())
            depth.update(payload)
        except Exception:
            pass

    await nc.subscribe(l1_subject, cb=handler_l1)
    await nc.subscribe(depth_subject, cb=handler_depth)

    print(f"Subscribed to L1={l1_subject}, DEPTH={depth_subject} on {nats_url}")

    while True:
        await asyncio.sleep(interval)
        l1_snap = l1.snapshot()
        depth_snap = depth.snapshot()
        l1_syms = set(l1_snap.keys())
        depth_syms = set(depth_snap.keys())
        both = l1_syms & depth_syms
        only_l1 = len(l1_syms - depth_syms)
        only_depth = len(depth_syms - l1_syms)
        print(f"\n--- Report @ {time.strftime('%H:%M:%S')} ---")
        print(f"L1 symbols: {len(l1_syms)}  Depth symbols: {len(depth_syms)}  Overlap: {len(both)}  L1-only: {only_l1}  Depth-only: {only_depth}")
        # sample a few overlapping symbols
        for (ex, sym) in list(both)[:5]:
            b1, a1, _ = l1_snap[(ex, sym)]
            b2, a2, _ = depth_snap[(ex, sym)]
            mid1 = (b1 + a1) / 2
            mid2 = (b2 + a2) / 2
            print(f"  {ex} {sym}: L1 mid={mid1:.6f}, Depth mid={mid2:.6f}, delta_bps={(mid2-mid1)/mid1*1e4:.3f}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
