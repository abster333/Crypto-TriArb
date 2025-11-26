from v5.state_store.manifest_validator import (
    validate_manifests,
    ManifestProblems,
    canonicalize_pair,
    validate_single_pool,
    to_checksum_address,
)
import pytest


def test_manifest_validation_passes(tmp_path):
    tokens = tmp_path / "tokens.json"
    pools = tmp_path / "pools.json"
    tokens.write_text('{"tokens": {"AAA": {"address": "0x' + "1"*40 + '", "decimals": 18, "chain_id":1}}}')
    pools.write_text('{"pairs": [{"id": "AAABBB", "token0": "AAA", "token1": "AAA", "pools": {"p": {"address": "0x' + "2"*40 + '", "metadata": {"network": "eth", "platform": "D", "fee":500}}}}]}')
    with pytest.raises(ManifestProblems):
        validate_manifests(str(tokens), str(pools))


def test_token_order_canonicalization():
    tokens = {"tokens": {"A": {"address": "0x" + "a"*40, "decimals": 18, "chain_id":1}, "B": {"address": "0x" + "1"*40, "decimals": 18, "chain_id":1}}}
    pair = {"id": "p", "token0": "A", "token1": "B"}
    out = canonicalize_pair(pair, tokens["tokens"])
    assert out["token0"] == "B" and out["token1"] == "A"


def test_validate_single_pool_fee_validation():
    issues = validate_single_pool({"address": "0x" + "3"*40, "metadata": {"fee": 999, "platform": "X", "network": "eth"}}, "p")
    assert issues


def test_checksum_conversion():
    checksummed = to_checksum_address("0x" + "4"*40)
    assert checksummed.startswith("0x") and len(checksummed) == 42
