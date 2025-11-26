"""Systematic pipeline diagnostics to find why pool freshness stays at zero.

Usage:
    PYTHONPATH=. python -m v5.tools.diagnose_pipeline
"""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from v5.common.config import Settings
from v5.ingest.ingest_service import IngestService
from v5.ingest.rpc_server import BatchRpcServer
from v5.state_store.hot_cache import HotCache
from v5.state_store.snapshot_store import SnapshotStore
from v5.state_store.state_reader import StateReader


async def test_1_manifest_loading():
    print("\n" + "=" * 60)
    print("TEST 1: Manifest Loading")
    print("=" * 60)
    settings = Settings()
    await settings.ensure_manifests(settings.generate_manifests)
    tokens = settings.load_tokens()
    pools = settings.load_pools()
    print(f"✓ Loaded {len(tokens)} tokens")
    print(f"✓ Loaded {len(pools)} pools")
    if pools:
        p = pools[0]
        print(f"Sample pool: {p.pool_address} {p.token0}/{p.token1} net={p.network} fee={p.fee_tier}")
    return tokens, pools


async def test_2_redis_connectivity():
    print("\n" + "=" * 60)
    print("TEST 2: Redis Connectivity")
    print("=" * 60)
    settings = Settings()
    print(f"Redis URL: {settings.redis_url}")
    hot = HotCache(settings.redis_url)
    snap = SnapshotStore(settings.redis_url)
    await hot.start()
    await snap.start()
    health = await hot.health()
    print(f"✓ Hot cache ping: {health}")
    await hot.close()
    await snap.close()
    return True


async def test_3_rpc_single_call():
    print("\n" + "=" * 60)
    print("TEST 3: RPC Single Call")
    print("=" * 60)
    settings = Settings()
    rpc = BatchRpcServer(max_calls_per_second=1, max_batch_size=5)
    await rpc.start()
    for net, url in (("eth", settings.ethereum_rpc_url), ("bsc", settings.bsc_rpc_url)):
        if not url:
            continue
        try:
            res = await rpc.submit_call(net, "eth_blockNumber", [])
            block_num = int(res, 16) if isinstance(res, str) and res.startswith("0x") else res
            print(f"✓ {net.upper()} blockNumber: {block_num}")
        except Exception as exc:  # noqa: BLE001
            print(f"✗ {net.upper()} RPC failed: {exc}")
    await rpc.stop()
    return True


async def test_4_poll_single_pool(pools):
    print("\n" + "=" * 60)
    print("TEST 4: Poll Single Pool")
    print("=" * 60)
    if not pools:
        print("✗ No pools available to test")
        return False
    pool = pools[0]
    settings = Settings()
    rpc = BatchRpcServer(max_calls_per_second=1, max_batch_size=3)
    await rpc.start()
    print(f"Polling pool {pool.pool_address} ({pool.token0}/{pool.token1} on {pool.network})")
    try:
        state = await rpc.submit_pool_state(pool.network, pool.pool_address)
        print(f"slot0: {state.get('slot0', 'missing')}")
        print(f"liquidity: {state.get('liquidity', 'missing')}")
        print(f"block_number: {state.get('block_number', 'missing')}")
        ok = bool(state.get("slot0") and state.get("liquidity"))
        print("✓ Pool data received" if ok else "✗ Pool data incomplete")
    except Exception as exc:  # noqa: BLE001
        print(f"✗ Poll failed: {exc}")
    await rpc.stop()
    return True


async def test_5_write_to_stores(pools):
    print("\n" + "=" * 60)
    print("TEST 5: Write to Stores")
    print("=" * 60)
    if not pools:
        print("✗ No pools available")
        return False
    settings = Settings()
    hot = HotCache(settings.redis_url)
    snap = SnapshotStore(settings.redis_url)
    await hot.start(); await snap.start()
    pool = pools[0]
    now_ms = int(time.time() * 1000)
    from v5.common.models import PoolState

    state = PoolState(
        pool_meta=pool,
        sqrt_price_x96=str(2**96),
        tick=0,
        liquidity=str(10**18),
        block_number=12345,
        timestamp_ms=now_ms,
        last_snapshot_ms=now_ms,
        last_ws_refresh_ms=now_ms,
        stream_ready=True,
    )
    await hot.write_pool_state(state)
    changed = await snap.write_if_changed(state)
    hot_data = await hot.read_pool_state(pool.pool_address)
    snap_data = await snap.read(pool.pool_address)
    print(f"✓ Hot write: {bool(hot_data)}; Snapshot write changed={changed}, has={bool(snap_data)}")
    await hot.close(); await snap.close()
    return bool(hot_data and snap_data)


async def test_6_full_ingest_loop(pools):
    print("\n" + "=" * 60)
    print("TEST 6: Full Ingest Loop (30s)")
    print("=" * 60)
    if not pools:
        print("✗ No pools available")
        return False
    settings = Settings()
    hot = HotCache(settings.redis_url)
    snap = SnapshotStore(settings.redis_url)
    await hot.start(); await snap.start()
    reader = StateReader(hot, snap)
    rpc = BatchRpcServer(max_calls_per_second=2, max_batch_size=10)
    await rpc.start()
    test_pools = pools[:5]
    ingest = IngestService(
        rpc,
        test_pools,
        hot,
        snap,
        uniswap_ws_url=settings.ethereum_ws_url or settings.alchemy_ws_url or "",
        pancake_ws_url=settings.bsc_ws_url or "",
        resync_interval_seconds=15,
        enable_poll_fallback=False,
    )
    await ingest.start()
    fresh_seen = False
    for i in range(6):  # 30 seconds total
        await asyncio.sleep(5)
        states = await reader.get_all_pool_states([p.pool_address for p in test_pools])
        now_ms = int(time.time() * 1000)
        count = sum(1 for s in states.values() if s)
        fresh = 0
        for st in states.values():
            ts = st.get("last_ws_refresh_ms") or st.get("last_snapshot_ms") or st.get("timestamp_ms") or 0
            if ts and now_ms - ts <= 30_000:
                fresh += 1
        print(f"[{(i+1)*5}s] pools_with_data={count}/{len(test_pools)} fresh={fresh}/{len(test_pools)}")
        if fresh:
            fresh_seen = True
    await ingest.stop()
    await rpc.stop()
    await hot.close(); await snap.close()
    return fresh_seen


async def test_7_ws_connectivity():
    print("\n" + "=" * 60)
    print("TEST 7: WebSocket Connectivity")
    print("=" * 60)
    settings = Settings()
    eth_ws = settings.ethereum_ws_url or settings.alchemy_ws_url
    bsc_ws = settings.bsc_ws_url
    if not eth_ws and not bsc_ws:
        print("✗ No WS URLs configured")
        return False
    import websockets
    if eth_ws and eth_ws.startswith("ws"):
        try:
            async with websockets.connect(eth_ws, ping_interval=10):
                print("✓ ETH WS connected")
        except Exception as exc:  # noqa: BLE001
            print(f"✗ ETH WS failed: {exc}")
    if bsc_ws and bsc_ws.startswith("ws"):
        try:
            async with websockets.connect(bsc_ws, ping_interval=10):
                print("✓ BSC WS connected")
        except Exception as exc:  # noqa: BLE001
            print(f"✗ BSC WS failed: {exc}")
    return True


async def main():
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print("\n" + "=" * 60)
    print("V5 PIPELINE DIAGNOSTICS")
    print("=" * 60)
    try:
        tokens, pools = await test_1_manifest_loading()
        await test_2_redis_connectivity()
        await test_3_rpc_single_call()
        await test_4_poll_single_pool(pools)
        await test_5_write_to_stores(pools)
        await test_6_full_ingest_loop(pools)
        await test_7_ws_connectivity()
    except Exception as exc:  # noqa: BLE001
        print(f"✗ Diagnostics failed: {exc}")
        import traceback
        traceback.print_exc()
    print("\n" + "=" * 60)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
