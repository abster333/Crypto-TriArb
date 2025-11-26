import pytest

from v5.common.config import validate_json_manifest


def test_token_schema_valid():
    payload = {"tokens": {"AAA": {"address": "0x" + "1"*40, "decimals": 18, "chain_id": 1, "symbol": "AAA"}}}
    validate_json_manifest(payload, "token_manifest.schema.json")


def test_token_schema_invalid_address():
    payload = {"tokens": {"AAA": {"address": "0x123", "decimals": 18, "chain_id": 1, "symbol": "AAA"}}}
    with pytest.raises(ValueError):
        validate_json_manifest(payload, "token_manifest.schema.json")


def test_pool_schema_valid():
    payload = {
        "pairs": [
            {
                "id": "AAABBB",
                "token0": "AAA",
                "token1": "BBB",
                "pools": {
                    "main": {
                        "address": "0x" + "2"*40,
                        "metadata": {"platform": "UNI", "network": "eth", "fee": 500}
                    }
                }
            }
        ]
    }
    validate_json_manifest(payload, "pool_manifest.schema.json")


def test_pool_schema_missing_metadata():
    payload = {"pairs": [{"id": "X", "token0": "A", "token1": "B", "pools": {"p": {"address": "0x" + "2"*40}}}]}
    with pytest.raises(ValueError):
        validate_json_manifest(payload, "pool_manifest.schema.json")
