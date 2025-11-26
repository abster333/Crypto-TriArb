import pytest
from v5.visibility.alarms import emit_health_alarms
from v5.common.models import AlarmConfig, HealthStatus


class DummySender:
    def __init__(self):
        self.called = 0
    async def __call__(self, cfg, health_list):
        self.called += 1


@pytest.mark.asyncio
async def test_alarm_dedup_and_send():
    cfg = AlarmConfig(webhook_urls=["http://example"])
    health = [HealthStatus(pool_address="0x" + "1"*40, is_stale=True, last_snapshot_ms=0, last_ws_ms=0, has_data=True)]
    sender = DummySender()
    await emit_health_alarms(cfg, health, sender=sender)
    assert sender.called == 1
