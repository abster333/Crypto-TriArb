"""V5 manifest generator using CoinGecko on-chain API.

Respects API constraints:
- per_page max 20
- max 10 pages (200 pools) per target
- 24h-volume ordering
- optional regen triggered via env V5_REGEN_MANIFEST=1
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Callable

MAX_PER_PAGE = 20
MAX_PAGES = 10
DEFAULT_DELAY = 3.0  # seconds between calls on free tier


@dataclass
class Target:
    network: str
    dex: str
    count: int


def log(msg: str) -> None:
    sys.stderr.write(msg + "\n")


def parse_targets(raw: str) -> List[Target]:
    targets: List[Target] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        toks = part.split(":")
        if len(toks) not in (2, 3):
            raise ValueError(f"Invalid target '{part}', expected network:dex[:count]")
        network, dex = toks[0], toks[1]
        count = int(toks[2]) if len(toks) == 3 and toks[2] else 50
        targets.append(Target(network=network, dex=dex, count=count))
    if not targets:
        targets.append(Target("eth", "uniswap_v3", 50))
    return targets


def fetch_page(network: str, dex: str, page: int, per_page: int, api_key: str) -> List[dict]:
    base = "https://api.coingecko.com/api/v3"
    url = f"{base}/onchain/networks/{network}/dexes/{dex}/pools?page={page}&per_page={per_page}"
    req = urllib.request.Request(url, headers={"x-cg-demo-api-key": api_key})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.load(resp)
    return payload.get("data") or []


def fetch_token(network: str, address: str, api_key: str) -> dict | None:
    base = "https://api.coingecko.com/api/v3"
    url = f"{base}/onchain/networks/{network}/tokens/{address}"
    req = urllib.request.Request(url, headers={"x-cg-demo-api-key": api_key})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.load(resp)
    return payload.get("data") or {}


def extract_token_ids(pool: dict) -> Tuple[str | None, str | None]:
    rel = pool.get("relationships") or {}
    base = ((rel.get("base_token") or {}).get("data") or {}).get("id")
    quote = ((rel.get("quote_token") or {}).get("data") or {}).get("id")
    return base, quote


def split_token_id(token_id: str | None) -> Tuple[str | None, str | None]:
    if not token_id or "_" not in token_id:
        return None, None
    network, addr = token_id.split("_", 1)
    return network, addr.lower()


def parse_fee_from_name(name: str) -> int | None:
    if not name or "%" not in name:
        return None
    try:
        pct = float(name.strip().split()[-1].rstrip("%"))
        return int(round(pct * 10_000))
    except Exception:
        return None


def rate_sleep(tier: str, sleep_fn: Callable[[float], None] = time.sleep) -> None:
    delay = DEFAULT_DELAY if tier != "paid" else 0.3
    sleep_fn(delay)


def fetch_pools_for_target(target: Target, api_key: str, tier: str, fetch_page_fn=fetch_page, sleep_fn=time.sleep) -> List[dict]:
    total_requested = min(target.count, MAX_PER_PAGE * MAX_PAGES)
    if target.count > total_requested:
        log(f"Requested {target.count} pools; capping to {total_requested} due to API limits")
    pages = math.ceil(total_requested / MAX_PER_PAGE)
    pools: List[dict] = []
    for page in range(1, pages + 1):
        remaining = total_requested - len(pools)
        per_page = min(MAX_PER_PAGE, remaining)
        log(f"  Page {page}/{pages}: fetching {per_page} entries")
        try:
            data = fetch_page_fn(target.network, target.dex, page, per_page, api_key)
        except urllib.error.HTTPError as exc:
            log(f"HTTP error page {page}: {exc}")
            break
        except urllib.error.URLError as exc:
            log(f"Network error page {page}: {exc}")
            break
        if not data:
            log("  Empty page, stopping early")
            break
        pools.extend(data)
        if len(pools) >= total_requested:
            break
        rate_sleep(tier, sleep_fn=sleep_fn)
    log(f"  Retrieved {len(pools)} pools (requested {total_requested})")
    return pools


def build_manifests(targets: List[Target], api_key: str, tier: str, *, fetch_page_fn=fetch_page, fetch_token_fn=fetch_token, skip_token_meta: bool = False, rate_sleep_fn=rate_sleep, sleep_fn=time.sleep) -> Tuple[dict, dict]:
    tokens: Dict[str, dict] = {}
    pairs: List[dict] = []
    token_cache: Dict[str, dict] = {}
    seen_pool_addresses: set[str] = set()

    for tgt in targets:
        log(f"Fetching pools for {tgt.network}/{tgt.dex} ({tgt.count} requested)")
        pools = fetch_pools_for_target(tgt, api_key, tier, fetch_page_fn=fetch_page_fn, sleep_fn=sleep_fn)
        for pool in pools:
            attrs = pool.get("attributes") or {}
            name = attrs.get("name") or ""
            fee = parse_fee_from_name(name) or 500
            address = (attrs.get("address") or "").lower()
            base_id, quote_id = extract_token_ids(pool)
            net0, addr0 = split_token_id(base_id)
            net1, addr1 = split_token_id(quote_id)
            if not (address and net0 and addr0 and net1 and addr1):
                continue

            for net, addr in ((net0, addr0), (net1, addr1)):
                if addr in token_cache:
                    continue
                symbol = addr[:6].upper()
                decimals = 18
                if not skip_token_meta:
                    try:
                        data = fetch_token_fn(net, addr, api_key)
                        attr = data.get("attributes") or {}
                        symbol = attr.get("symbol") or symbol
                        decimals = attr.get("decimals", decimals)
                    except Exception:
                        pass
                token_cache[addr] = {"symbol": symbol.upper(), "address": addr, "decimals": int(decimals), "chain_id": 1 if net == "eth" else 56, "chain": net}
                rate_sleep_fn(tier, sleep_fn=sleep_fn)

            sym0 = token_cache.get(addr0, {}).get("symbol")
            sym1 = token_cache.get(addr1, {}).get("symbol")
            if not sym0 or not sym1:
                continue
            # Keep per-chain keys to avoid clobbering symbols that exist on multiple networks
            tokens[f"{sym0}:{net0}"] = token_cache[addr0]
            tokens[f"{sym1}:{net1}"] = token_cache[addr1]
            # Preserve first-seen plain symbol for backward compatibility
            tokens.setdefault(sym0, token_cache[addr0])
            tokens.setdefault(sym1, token_cache[addr1])

            addr_l = address.lower()
            if addr_l in seen_pool_addresses:
                continue
            seen_pool_addresses.add(addr_l)

            pair_entry = {
                "id": f"{sym0}{sym1}",
                "token0": sym0,
                "token1": sym1,
                "pools": {
                    tgt.dex: {
                        "address": address,
                        "metadata": {
                            "platform": tgt.dex.upper(),
                            "network": tgt.network,
                            "fee": fee,
                        },
                    }
                },
            }
            pairs.append(pair_entry)
    return {"tokens": tokens}, {"pairs": pairs}


def maybe_regen(env_flag: str) -> bool:
    return env_flag.strip() == "1"


def main() -> None:
    api_key = os.environ.get("COINGECKO_API_KEY") or os.environ.get("COINGECKO_DEMO_API_KEY") or "CG-Lj3zoN2AMeZ9h9F94ew6XK7V"
    targets_env = os.environ.get("V5_TARGETS", "eth:uniswap_v3:50,bsc:pancakeswap-v3-bsc:50")
    tier = os.environ.get("COINGECKO_TIER", "free").lower()
    skip_meta = os.environ.get("COINGECKO_SKIP_TOKEN_META", "0") == "1"
    targets = parse_targets(targets_env)
    tokens_out = Path(os.environ.get("TOKEN_MANIFEST_PATH", "config/v5/tokens.json"))
    pools_out = Path(os.environ.get("POOL_MANIFEST_PATH", "config/v5/pools.json"))

    log(f"Generating manifests for {len(targets)} target(s)")
    tokens, pools = build_manifests(targets, api_key, tier, skip_token_meta=skip_meta)

    tokens_out.parent.mkdir(parents=True, exist_ok=True)
    pools_out.parent.mkdir(parents=True, exist_ok=True)
    tokens_out.write_text(json.dumps(tokens, indent=2))
    pools_out.write_text(json.dumps(pools, indent=2))
    log(f"✓ Generated {len(tokens['tokens'])} tokens -> {tokens_out}")
    log(f"✓ Generated {len(pools['pairs'])} pairs -> {pools_out}")


if __name__ == "__main__":
    main()
