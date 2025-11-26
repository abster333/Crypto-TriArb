"""Decode DEX swap events."""

from __future__ import annotations

from typing import Optional

# Uniswap v3 Swap event topic
# event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1,
#            uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
SWAP_TOPIC_UNIV3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# Pancake v3 Swap topic (BSC)
SWAP_TOPIC_PANCAKE_V3 = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"


def decode_swap(log: dict) -> Optional[dict]:
    """Decode a Uniswap/Pancake v3 Swap log into structured fields.

    Returns dict with pool, block_number, sqrt_price_x96, liquidity, tick.
    """
    if not log:
        return None
    topics = log.get("topics") or []
    if not topics:
        return None
    if topics[0].lower() not in (SWAP_TOPIC_UNIV3, SWAP_TOPIC_PANCAKE_V3):
        return None
    data = log.get("data") or ""
    if not isinstance(data, str) or not data.startswith("0x"):
        return None
    payload = data[2:]
    # Expect at least 5 words -> amount0, amount1, sqrtPriceX96, liquidity, tick
    if len(payload) < 64 * 5:
        return None
    try:
        sqrt_price_x96 = int(payload[64 * 2 : 64 * 3] or "0", 16)
        liquidity = int(payload[64 * 3 : 64 * 4] or "0", 16)
        tick_raw = int(payload[64 * 4 : 64 * 5] or "0", 16)
        if tick_raw >= 2**255:
            tick_raw -= 2**256
    except ValueError:
        return None
    if sqrt_price_x96 == 0:
        # fallback to next word if layout differs
        try:
            sqrt_price_x96 = int(payload[64 * 1 : 64 * 2] or "0", 16)
        except ValueError:
            sqrt_price_x96 = 1
    if sqrt_price_x96 == 0:
        sqrt_price_x96 = 1
    if liquidity == 0:
        liquidity = 1
    # blockNumber may be hex str or int
    block_raw = log.get("blockNumber") or 0
    try:
        block_number = int(block_raw, 16) if isinstance(block_raw, str) and block_raw.startswith("0x") else int(block_raw)
    except Exception:
        block_number = 0

    return {
        "pool": log.get("address", "").lower(),
        "block_number": block_number,
        "sqrt_price_x96": sqrt_price_x96,
        "liquidity": liquidity,
        "tick": tick_raw,
    }


__all__ = ["decode_swap", "SWAP_TOPIC_UNIV3", "SWAP_TOPIC_PANCAKE_V3"]
