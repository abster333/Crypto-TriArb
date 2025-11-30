"""Manifest validation with canonicalization and rich errors."""

from __future__ import annotations

import re
from typing import Dict, Set

from v5.common.config import validate_json_manifest, _load_json


HEX_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
# Allowed fee tiers (in bps) across supported DEXes.
# PancakeSwap v3 commonly uses 25, 100, 500, 2500, 10000; Uniswap v3 uses 100, 500, 3000, 10000 for the pools we track.
VALID_FEE_TIERS = {25, 100, 250, 500, 2500, 3000, 10000}


class ManifestProblems(Exception):
    def __init__(self, issues: list[str]):
        super().__init__("; ".join(issues))
        self.issues = issues


def to_checksum_address(addr: str) -> str:
    """Compute EIP-55 checksum."""
    if not HEX_ADDR_RE.match(addr):
        raise ValueError(f"invalid address format: {addr}")
    addr_lower = addr.lower().replace("0x", "")
    import hashlib

    hash_hex = hashlib.sha3_256(addr_lower.encode()).hexdigest()
    result = "0x"
    for ch, h in zip(addr_lower, hash_hex):
        result += ch.upper() if int(h, 16) >= 8 else ch
    return result


def canonicalize_pair(pair: Dict, tokens: Dict[str, Dict]) -> Dict:
    """Ensure token order follows address lexicographic order."""
    token0 = pair["token0"]
    token1 = pair["token1"]
    addr0 = tokens.get(token0, {}).get("address")
    addr1 = tokens.get(token1, {}).get("address")
    if addr0 and addr1 and addr1.lower() < addr0.lower():
        pair["token0"], pair["token1"] = token1, token0
    return pair


def validate_single_pool(pool_meta: Dict, pair_id: str) -> list[str]:
    issues: list[str] = []
    addr = pool_meta.get("address", "")
    if not HEX_ADDR_RE.match(addr or ""):
        issues.append(f"{pair_id}: pool address invalid {addr}")
    try:
        pool_meta["address"] = to_checksum_address(addr)
    except Exception:
        pass
    meta = pool_meta.get("metadata") or {}
    fee = meta.get("fee")
    if fee not in VALID_FEE_TIERS:
        issues.append(f"{pair_id}: unsupported fee {fee}")
    return issues


def validate_manifests(tokens_path: str, pools_path: str) -> None:
    issues: list[str] = []
    tokens = _load_json(tokens_path)
    pools = _load_json(pools_path)
    try:
        validate_json_manifest(tokens, "token_manifest.schema.json")
    except ValueError as exc:
        issues.append(f"token schema: {exc}")
    try:
        validate_json_manifest(pools, "pool_manifest.schema.json")
    except ValueError as exc:
        issues.append(f"pool schema: {exc}")
    issues.extend(_semantic_checks(tokens, pools))
    if issues:
        raise ManifestProblems(issues)


def _semantic_checks(tokens: Dict, pools: Dict) -> list[str]:
    problems: list[str] = []
    token_set = set(tokens.get("tokens", {}).keys())
    seen_addresses: Set[str] = set()
    for pair in pools.get("pairs", []):
        pair_id = pair.get("id", "<unknown>")
        canonicalize_pair(pair, tokens.get("tokens", {}))
        t0, t1 = pair.get("token0"), pair.get("token1")
        if t0 not in token_set or t1 not in token_set:
            problems.append(f"pair {pair_id} references unknown token")
        for pool_key, meta in (pair.get("pools") or {}).items():
            addr = str(meta.get("address", "")).lower()
            if not addr:
                problems.append(f"pair {pair_id} pool {pool_key} missing address")
                continue
            if addr in seen_addresses:
                problems.append(f"duplicate pool address {addr}")
            seen_addresses.add(addr)
            network = ((meta.get("metadata") or {}).get("network") or "").lower()
            if not network:
                problems.append(f"pool {addr} missing network")
            problems.extend(validate_single_pool(meta, pair_id))
    return problems


__all__ = ["validate_manifests", "ManifestProblems", "validate_single_pool", "to_checksum_address"]
