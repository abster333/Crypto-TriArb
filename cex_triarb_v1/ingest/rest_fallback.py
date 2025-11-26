from __future__ import annotations

import asyncio
import logging
import time
from typing import Iterable, Optional, Tuple

import aiohttp

log = logging.getLogger(__name__)

COINBASE_TICKER = "https://api.exchange.coinbase.com/products/{product}/ticker"
KRAKEN_TICKER = "https://api.kraken.com/0/public/Ticker"
REST_DELAY_SEC = 0.5  # light built-in pacing to avoid hammering endpoints


class RateLimitError(Exception):
    """Signal that an exchange returned HTTP 429."""

    def __init__(self, exchange: str):
        super().__init__(f"Rate limit from {exchange}")
        self.exchange = exchange.upper()


async def fetch_coinbase(symbol: str) -> Tuple[Optional[float], float]:
    product = _to_coinbase_product(symbol)
    url = COINBASE_TICKER.format(product=product)
    start = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 429:
                    raise RateLimitError("coinbase")
                if resp.status != 200:
                    log.warning("Coinbase REST %s status=%s", product, resp.status)
                    return None, _elapsed_ms(start)
                data = await resp.json()
                return float(data["price"]), _elapsed_ms(start)
    except RateLimitError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.debug("Coinbase REST error for %s: %s", product, exc)
        return None, _elapsed_ms(start)


async def fetch_kraken(symbol: str) -> Tuple[Optional[float], float]:
    pair = _to_kraken_pair(symbol)
    start = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(KRAKEN_TICKER, params={"pair": pair}, timeout=5) as resp:
                if resp.status == 429:
                    raise RateLimitError("kraken")
                if resp.status != 200:
                    log.warning("Kraken REST %s status=%s", pair, resp.status)
                    return None, _elapsed_ms(start)
                data = await resp.json()
        if data.get("error"):
            log.warning("Kraken REST error for %s: %s", pair, data["error"])
            return None, _elapsed_ms(start)
        result = data["result"]
        ticker = next(iter(result.values()))
        last = ticker["c"][0]
        return float(last), _elapsed_ms(start)
    except RateLimitError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.debug("Kraken REST error for %s: %s", pair, exc)
        return None, _elapsed_ms(start)


async def rest_refresh(symbols: Iterable[str], exchanges=("COINBASE", "KRAKEN")) -> dict:
    """Lightweight REST refresh with a fixed small delay between calls."""
    out = {}
    for sym in symbols:
        if "COINBASE" in exchanges:
            try:
                price, latency = await fetch_coinbase(sym)
                if price is not None:
                    out[("COINBASE", sym)] = (price, latency)
            except RateLimitError as exc:
                raise exc
            await asyncio.sleep(REST_DELAY_SEC)
        if "KRAKEN" in exchanges:
            try:
                price, latency = await fetch_kraken(sym)
                if price is not None:
                    out[("KRAKEN", sym)] = (price, latency)
            except RateLimitError as exc:
                raise exc
            await asyncio.sleep(REST_DELAY_SEC)
    return out


async def _wrap(exchange: str, symbol: str, coro):
    price, latency = await coro
    if price is None:
        return None
    return (exchange, symbol, price, latency)


def _to_coinbase_product(symbol: str) -> str:
    symbol = symbol.upper()
    if "-" in symbol:
        return symbol
    for suffix in ("USD", "USDT", "USDC", "BTC", "ETH"):
        if symbol.endswith(suffix):
            base = symbol[: -len(suffix)]
            return f"{base}-{suffix}"
    raise ValueError(f"Unsupported Coinbase symbol {symbol}")


def _to_kraken_pair(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "")
    for suffix in ("USDT", "USDC", "USD", "BTC", "ETH"):
        if symbol.endswith(suffix):
            base, quote = symbol[: -len(suffix)], symbol[-len(suffix) :]
            if base == "BTC":
                base = "XBT"
            # Kraken REST expects compact form, e.g. XBTUSD (no slash).
            return f"{base}{quote}"
    raise ValueError(f"Unsupported Kraken symbol {symbol}")


def _elapsed_ms(start: float) -> float:
    return (time.time() - start) * 1000.0
