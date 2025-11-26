import pytest

from v5.common.config import validate_json_manifest


def test_ws_schema_valid():
    payload = {"networks": {"eth": {"url": "wss://example", "topics": ["swap"], "reconnect_delay": 1}}}
    validate_json_manifest(payload, "ws_config.schema.json")


def test_ws_schema_missing_url():
    payload = {"networks": {"eth": {"topics": ["swap"]}}}
    with pytest.raises(ValueError):
        validate_json_manifest(payload, "ws_config.schema.json")


def test_alarms_schema_valid():
    payload = {"thresholds": {"stale_snapshot_ms": 1, "stale_ws_ms": 1, "missing_data": 0}, "webhooks": []}
    validate_json_manifest(payload, "alarms.schema.json")


def test_alarms_schema_missing_thresholds():
    with pytest.raises(ValueError):
        validate_json_manifest({}, "alarms.schema.json")
