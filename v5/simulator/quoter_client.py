"""Lightweight Pancake/Uniswap V3 quoter client using JSON-RPC eth_call.

We avoid web3 dependency and use eth-abi directly.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

import aiohttp
from eth_abi import encode, decode

log = logging.getLogger(__name__)

# Pancake V3 Quoter V2 on BSC mainnet (known working in our tests)
DEFAULT_QUOTER = "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997"


@dataclass
class QuoterResult:
    amount_out: int
    sqrt_price_after: int
    initialized_ticks_crossed: int
    gas_estimate: int


async def quote_exact_input_single(
    rpc_url: str,
    token_in: str,
    token_out: str,
    fee: int,
    amount_in: int,
    quoter: str = DEFAULT_QUOTER,
    sqrt_price_limit_x96: int = 0,
    session: aiohttp.ClientSession | None = None,
) -> Optional[QuoterResult]:
    """Call QuoterV2.quoteExactInputSingle; returns None on failure."""
    # function signature: quoteExactInputSingle(address,address,uint24,uint256,uint160)
    selector = "0xc6a5026a"  # quoteExactInputSingle
    try:
        calldata = (
            selector
            + encode(
                ["address", "address", "uint24", "uint256", "uint160"],
                [token_in, token_out, fee, amount_in, sqrt_price_limit_x96],
            ).hex()
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("quoter encode failed: %s", exc)
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {"to": quoter, "data": calldata},
            "latest",
        ],
    }
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    try:
        async with session.post(rpc_url, json=payload, timeout=15) as resp:
            if resp.status != 200:
                log.debug("quoter http %s", resp.status)
                return None
            body = await resp.json()
    except Exception as exc:  # noqa: BLE001
        log.debug("quoter request failed: %s", exc)
        return None
    finally:
        if close_session:
            await session.close()

    if "result" not in body:
        log.debug("quoter no result: %s", body)
        return None
    try:
        decoded = decode(
            ["uint256", "uint160", "int24", "uint256"],
            bytes.fromhex(body["result"].removeprefix("0x")),
        )
        return QuoterResult(
            amount_out=int(decoded[0]),
            sqrt_price_after=int(decoded[1]),
            initialized_ticks_crossed=int(decoded[2]),
            gas_estimate=int(decoded[3]),
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("quoter decode failed: %s", exc)
        return None


async def quote_exact_input(
    rpc_url: str,
    path_tokens: list[str],
    path_fees: list[int],
    amount_in: int,
    quoter: str = DEFAULT_QUOTER,
    session: aiohttp.ClientSession | None = None,
) -> Optional[QuoterResult]:
    """Call QuoterV2.quoteExactInput(path, amountIn). Path = tokenIn, fee, tokenOut (packed)."""
    if len(path_tokens) != len(path_fees) + 1:
        return None
    selector = "0xcdca1753"  # quoteExactInput(bytes, uint256)
    # build path: token0 (20 bytes) + fee (3 bytes) + token1 (20) [+ fee + tokenN]
    try:
        parts = []
        for i, tok in enumerate(path_tokens):
            parts.append(tok[2:].lower())
            if i < len(path_fees):
                fee_hex = format(path_fees[i], "06x")
                parts.append(fee_hex)
        path_hex = "".join(parts)
        calldata = selector + encode(["bytes", "uint256"], [bytes.fromhex(path_hex), amount_in]).hex()
    except Exception as exc:  # noqa: BLE001
        log.debug("quoter encode path failed: %s", exc)
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {"to": quoter, "data": calldata},
            "latest",
        ],
    }
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True
    try:
        async with session.post(rpc_url, json=payload, timeout=15) as resp:
            if resp.status != 200:
                return None
            body = await resp.json()
    except Exception:
        return None
    finally:
        if close_session:
            await session.close()

    if "result" not in body:
        return None
    try:
        decoded = decode(
            ["uint256", "uint160", "int24", "uint256"],
            bytes.fromhex(body["result"].removeprefix("0x")),
        )
        return QuoterResult(
            amount_out=int(decoded[0]),
            sqrt_price_after=int(decoded[1]),
            initialized_ticks_crossed=int(decoded[2]),
            gas_estimate=int(decoded[3]),
        )
    except Exception:
        return None


__all__ = ["quote_exact_input_single", "QuoterResult", "DEFAULT_QUOTER"]
