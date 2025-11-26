from v5.visibility.health_tracker import HealthTracker
from v5.common.models import PoolState, PoolMeta


def test_health_tracker_flags_stale():
    tracker = HealthTracker(stale_snapshot_ms=1, stale_stream_ms=1)
    meta = PoolMeta(pool_address="0x" + "c"*40, platform="D", token0="A", token1="B", fee_tier=500, network="eth")
    stale_pool = PoolState(
        pool_meta=meta,
        sqrt_price_x96="0x1",
        tick=0,
        liquidity="0x2",
        block_number=1,
        timestamp_ms=1,
        last_snapshot_ms=0,
        last_ws_refresh_ms=0,
    )
    health = tracker.evaluate([stale_pool])
    assert len(health) == 1
    assert health[0].is_stale is True
