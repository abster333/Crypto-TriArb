from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, Iterable, List, Set, Tuple

import aiohttp

log = logging.getLogger(__name__)

COINBASE_PRODUCTS = "https://api.exchange.coinbase.com/products"
KRAKEN_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
OKX_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"

QUOTE_WHITELIST = {"USD", "USDT", "USDC", "BTC", "ETH"}
STABLES = {"USD", "USDT", "USDC"}


def _quote_whitelist_for(exchange: str) -> Set[str]:
    env_key = f"{exchange.upper()}_QUOTES"
    env_val = os.getenv(env_key)
    if env_val:
        return {q.strip().upper() for q in env_val.split(",") if q.strip()}
    return set(QUOTE_WHITELIST)


async def fetch_coinbase_symbols() -> List[str]:
    quotes = _quote_whitelist_for("COINBASE")
    out: List[str] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(COINBASE_PRODUCTS, timeout=10) as resp:
                if resp.status != 200:
                    log.warning("Coinbase products status=%s", resp.status)
                    return out
                data = await resp.json()
        for prod in data:
            status = prod.get("status", "online").lower()
            if status not in ("online", "online_trading"):
                continue
            base = (prod.get("base_currency") or "").upper()
            quote = (prod.get("quote_currency") or "").upper()
            if not base or quote not in quotes:
                continue
            # Skip stable-stable pairs that create bogus triangles (e.g., USDTUSD).
            if base in STABLES and quote in STABLES:
                continue
            out.append(f"{base}{quote}")
    except Exception as exc:  # noqa: BLE001
        log.warning("Coinbase symbol fetch failed: %s", exc)
    return out


async def fetch_kraken_symbols() -> List[str]:
    quotes = _quote_whitelist_for("KRAKEN")
    out: List[str] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(KRAKEN_PAIRS, timeout=10) as resp:
                if resp.status != 200:
                    log.warning("Kraken pairs status=%s", resp.status)
                    return out
                payload = await resp.json()
        result = payload.get("result", {}) or {}
        for _, info in result.items():
            if info.get("status", "online").lower() not in ("online", "online_trading"):
                continue
            wsname = info.get("wsname") or ""
            if "/" not in wsname:
                continue
            base, quote = wsname.split("/", 1)
            base = base.upper()
            quote = quote.upper()
            if quote not in quotes:
                continue
            if base in STABLES and quote in STABLES:
                continue
            out.append(f"{base}{quote}")
    except Exception as exc:  # noqa: BLE001
        log.warning("Kraken symbol fetch failed: %s", exc)
    return out


async def fetch_okx_symbols() -> List[str]:
    """Fetch tradable spot instruments from OKX and normalize to BASEQUOTE."""
    quotes = _quote_whitelist_for("OKX")
    out: List[str] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(OKX_INSTRUMENTS, timeout=10) as resp:
                if resp.status != 200:
                    log.warning("OKX instruments status=%s", resp.status)
                    return out
                payload = await resp.json()
        data = payload.get("data") or []
        for inst in data:
            if (inst.get("state") or "").lower() != "live":
                continue
            inst_id = inst.get("instId") or ""
            if "-" not in inst_id:
                continue
            base, quote = inst_id.split("-", 1)
            base = _norm_base(base)
            quote = _norm_quote(quote)
            if quote not in quotes:
                continue
            if base in STABLES and quote in STABLES:
                continue
            out.append(f"{base}{quote}")
    except Exception as exc:  # noqa: BLE001
        log.warning("OKX symbol fetch failed: %s", exc)
    return out


def _norm_base(raw: str) -> str:
    raw = raw.upper()
    # Kraken prefixes: X for crypto, Z for fiat. Strip one leading marker.
    if raw.startswith(("X", "Z")):
        raw = raw[1:]
    if raw in ("XBT", "XXBT", "BT"):
        return "BTC"
    return raw.replace("/", "")


def _norm_quote(raw: str) -> str:
    raw = raw.upper().replace("/", "")
    if raw.startswith("Z"):
        raw = raw[1:]
    if raw == "XBT":
        return "BTC"
    return raw


async def discover_symbols(exchanges: Iterable[str]) -> List[str]:
    tasks = []
    ex_set = {ex.upper() for ex in exchanges}
    if "COINBASE" in ex_set:
        tasks.append(fetch_coinbase_symbols())
    if "KRAKEN" in ex_set:
        tasks.append(fetch_kraken_symbols())
    if "OKX" in ex_set:
        tasks.append(fetch_okx_symbols())
    results = await asyncio.gather(*tasks, return_exceptions=True)
    merged: Set[str] = set()
    for res in results:
        if isinstance(res, Exception):
            continue
        merged.update(res)
    return sorted(merged)


async def discover_per_exchange(exchanges: Iterable[str]) -> Tuple[List[str], Dict[str, List[str]]]:
    """Return (union, per-exchange map) of symbols."""
    ex_set = {ex.upper() for ex in exchanges}
    per: Dict[str, List[str]] = {}
    if "COINBASE" in ex_set:
        per["COINBASE"] = await fetch_coinbase_symbols()
    if "KRAKEN" in ex_set:
        per["KRAKEN"] = await fetch_kraken_symbols()
    if "OKX" in ex_set:
        per["OKX"] = await fetch_okx_symbols()
    union: Set[str] = set()
    for lst in per.values():
        union.update(lst)
    return sorted(union), per
