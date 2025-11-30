"""Async CoinGecko manifest generator with non-blocking rate limiting."""

from __future__ import annotations

import asyncio
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import aiohttp
import logging

MAX_PER_PAGE = 20
MAX_PAGES = 10
log = logging.getLogger(__name__)


@dataclass
class Target:
    network: str
    dex: str
    count: int


def parse_targets(raw: str) -> List[Target]:
    targets: List[Target] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        toks = part.split(":")
        count = 50
        if len(toks) >= 3 and toks[2].strip():
            count = int(toks[2])
        targets.append(Target(toks[0], toks[1], count))
    if not targets:
        targets.append(Target("eth", "uniswap_v3", 50))
    return targets


def _clean_symbol(value: str | None, fallback: str) -> str | None:
    """Return an uppercase ASCII symbol or None if it cannot be sanitized."""
    candidate = (value or fallback or "").strip().upper()
    if not candidate:
        return None
    if candidate.isascii():
        return candidate
    filtered = "".join(ch for ch in candidate if ch.isascii() and (ch.isalnum() or ch in {"_", "-"}))
    filtered = filtered.strip("_-")
    return filtered or None


async def rate_sleep(tier: str) -> None:
    # Slow down on free/demo keys to avoid 429s
    delay = 3.0 if tier != "paid" else 0.3
    await asyncio.sleep(delay)


async def fetch_page(session: aiohttp.ClientSession, network: str, dex: str, page: int, per_page: int, api_key: str) -> List[dict]:
    url = f"https://api.coingecko.com/api/v3/onchain/networks/{network}/dexes/{dex}/pools?page={page}&per_page={per_page}"
    headers = {"x-cg-demo-api-key": api_key}
    async with session.get(url, headers=headers, timeout=30) as resp:
        status = resp.status
        text = await resp.text()
        if status == 429:
            log.warning("CoinGecko rate limit hit (429) for %s page=%s; backing off", dex, page)
            await asyncio.sleep(2)
            return []
        if status >= 400:
            log.warning("CoinGecko HTTP %s for %s page=%s body=%s", status, dex, page, text[:200])
            return []
        try:
            payload = json.loads(text)
        except Exception:
            log.warning("CoinGecko decode error for %s page=%s", dex, page)
            return []
    return payload.get("data") or []


async def fetch_token(session: aiohttp.ClientSession, network: str, addr: str, api_key: str) -> dict | None:
    url = f"https://api.coingecko.com/api/v3/onchain/networks/{network}/tokens/{addr}"
    headers = {"x-cg-demo-api-key": api_key}
    async with session.get(url, headers=headers, timeout=20) as resp:
        payload = await resp.json()
    return payload.get("data")


async def fetch_pools_for_target(session: aiohttp.ClientSession, target: Target, api_key: str, tier: str) -> List[dict]:
    total_requested = min(target.count, MAX_PER_PAGE * MAX_PAGES)
    pages = math.ceil(total_requested / MAX_PER_PAGE)
    pools: List[dict] = []
    for page in range(1, pages + 1):
        remaining = total_requested - len(pools)
        per_page = min(MAX_PER_PAGE, remaining)
        data = await fetch_page(session, target.network, target.dex, page, per_page, api_key)
        if not data:
            break
        pools.extend(data)
        if len(pools) >= total_requested:
            break
        await rate_sleep(tier)
    return pools


async def build_manifests_async(targets: List[Target], api_key: str, tier: str) -> Tuple[dict, dict]:
    tokens: Dict[str, dict] = {}
    pairs: List[dict] = []
    token_cache: Dict[str, dict] = {}
    invalid_tokens: set[str] = set()
    seen_pool_addresses: set[str] = set()
    async with aiohttp.ClientSession() as session:
        for tgt in targets:
            pools = await fetch_pools_for_target(session, tgt, api_key, tier)
            for pool in pools:
                rel = pool.get("relationships") or {}
                base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id")
                quote_id = ((rel.get("quote_token") or {}).get("data") or {}).get("id")
                addr = (pool.get("attributes") or {}).get("address", "")
                name = (pool.get("attributes") or {}).get("name", "")
                fee = 500
                if name and "%" in name.split()[-1]:
                    try:
                        pct = float(name.split()[-1].rstrip("%"))
                        fee = int(round(pct * 10_000))
                    except Exception:
                        pass
                def split(tok):
                    if tok and "_" in tok:
                        n, a = tok.split("_", 1)
                        return n, a.lower()
                    return None, None
                net0, addr0 = split(base_id)
                net1, addr1 = split(quote_id)
                if not (addr and net0 and addr0 and net1 and addr1):
                    continue
                for net, a in ((net0, addr0), (net1, addr1)):
                    if not a or a in token_cache or a in invalid_tokens:
                        continue
                    default_symbol = a[:6].upper()
                    symbol = default_symbol
                    decimals = 18
                    try:
                        data = await fetch_token(session, net, a, api_key)
                        attr = (data or {}).get("attributes") or {}
                        symbol = attr.get("symbol", symbol)
                        decimals = attr.get("decimals", decimals)
                    except Exception:
                        pass
                    cleaned_symbol = _clean_symbol(symbol, default_symbol)
                    if not cleaned_symbol:
                        invalid_tokens.add(a)
                        log.warning("Skipping token %s on %s due to non-ASCII symbol %r", a, net, symbol)
                        await rate_sleep(tier)
                        continue
                    token_cache[a] = {
                        "symbol": cleaned_symbol,
                        "address": a,
                        "decimals": int(decimals),
                        "chain_id": 1 if net == "eth" else 56,
                        "chain": net,
                    }
                    await rate_sleep(tier)
                sym0 = token_cache.get(addr0, {}).get("symbol")
                sym1 = token_cache.get(addr1, {}).get("symbol")
                if not sym0 or not sym1:
                    continue
                # Store tokens per chain to avoid symbol collisions (e.g., USDC on ETH vs BSC)
                tokens[f"{sym0}:{net0}"] = token_cache[addr0]
                tokens[f"{sym1}:{net1}"] = token_cache[addr1]
                # Also keep the first-seen symbol for backwards compatibility
                tokens.setdefault(sym0, token_cache[addr0])
                tokens.setdefault(sym1, token_cache[addr1])

                # Skip duplicate pool addresses (CoinGecko sometimes returns dup entries)
                addr_l = addr.lower()
                if addr_l in seen_pool_addresses:
                    continue
                seen_pool_addresses.add(addr_l)

                pairs.append(
                    {
                        "id": f"{sym0}{sym1}",
                        "token0": sym0,
                        "token1": sym1,
                        "pools": {
                            tgt.dex: {
                                "address": addr,
                                "metadata": {"platform": tgt.dex.upper(), "network": tgt.network, "fee": fee},
                            }
                        },
                    }
                )
    return {"tokens": tokens}, {"pairs": pairs}


def main() -> None:
    api_key = os.environ.get("COINGECKO_API_KEY") or os.environ.get("COINGECKO_DEMO_API_KEY") or "CG-Lj3zoN2AMeZ9h9F94ew6XK7V"
    tier = os.environ.get("COINGECKO_TIER", "free")
    targets = parse_targets(os.environ.get("V5_TARGETS", "eth:uniswap_v3:50,bsc:pancakeswap-v3-bsc:50"))
    tokens_out = Path(os.environ.get("TOKEN_MANIFEST_PATH", "config/v5/tokens.json"))
    pools_out = Path(os.environ.get("POOL_MANIFEST_PATH", "config/v5/pools.json"))
    tokens, pools = asyncio.run(build_manifests_async(targets, api_key, tier))
    tokens_out.parent.mkdir(parents=True, exist_ok=True)
    pools_out.parent.mkdir(parents=True, exist_ok=True)
    tokens_out.write_text(json.dumps(tokens, indent=2))
    pools_out.write_text(json.dumps(pools, indent=2))


if __name__ == "__main__":
    main()
