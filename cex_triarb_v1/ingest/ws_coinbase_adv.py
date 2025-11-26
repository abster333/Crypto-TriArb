from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
import os
import textwrap
from typing import Awaitable, Callable, Optional, Sequence

import jwt
import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .depth import DepthBook, DepthSnapshot
from .ws_base import BaseWsAdapter
from .ws_coinbase import CoinbaseTicker
from . import metrics

log = logging.getLogger(__name__)

_DEFAULT_URL = "wss://advanced-trade-ws.coinbase.com"


def _parse_ts(ts: str | None) -> int:
    if not ts:
        return int(time.time() * 1000)
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


class CoinbaseAdvWsAdapter(BaseWsAdapter):
    """Coinbase Advanced Trade WS adapter (ticker/level2)."""

    def __init__(
        self,
        symbols: Sequence[str],
        on_ticker: Optional[Callable[[object], Awaitable[None]]] = None,
        on_depth: Optional[Callable[[DepthSnapshot], Awaitable[None]]] = None,
        url: str = _DEFAULT_URL,
        session_factory: Callable[..., Awaitable] | None = None,
        channels: Sequence[str] | None = None,
        depth_levels: int = 5,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        super().__init__("COINBASE_ADV", symbols)
        self.url = url
        self.session_factory = session_factory or (
            lambda u: websockets.connect(u, ping_interval=15, ping_timeout=45, close_timeout=15, max_size=5_000_000)
        )
        self._on_ticker = on_ticker
        self._on_depth = on_depth
        self._channels = list(channels) if channels else ["level2"]
        self._depth_levels = max(1, depth_levels)
        self._depth_books: dict[str, DepthBook] = {}
        self._api_key = api_key
        self._api_secret = api_secret

    async def run_forever(self) -> None:  # pragma: no cover
        attempt = 0
        while True:
            try:
                attempt += 1
                sym_list = ",".join(self.symbols)
                log.info("Coinbase ADV WS connecting (attempt %d) symbols=%s", attempt, sym_list)
                async with self.session_factory(self.url) as ws:
                    await self._subscribe(ws)
                    async for message in ws:
                        await self.handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Coinbase ADV WS loop error (attempt %d): %s", attempt, exc)
                metrics.WS_RECONNECTS.labels(exchange="COINBASE_ADV").inc()
                await asyncio.sleep(1.5)

    async def _subscribe(self, ws) -> None:
        jwt_token = self._generate_jwt()
        for channel in self._channels:
            body = {
                "type": "subscribe",
                "channel": channel,
                "product_ids": [self._to_product_id(s) for s in self.symbols],
            }
            if jwt_token:
                body["jwt"] = jwt_token
            await ws.send(json.dumps(body))

    def _generate_jwt(self) -> str | None:
        api_key = self._api_key
        api_secret = self._api_secret
        # If key not provided but secret is a JSON file, try to read "name".
        if not api_key and api_secret and os.path.exists(api_secret) and api_secret.lower().endswith(".json"):
            try:
                import json

                with open(api_secret, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    api_key = data.get("name", api_key)
            except Exception:
                pass
        if not (api_key and api_secret):
            log.warning("Coinbase ADV auth missing api_key or api_secret")
            return None
        secrets = self._load_secret_candidates(api_secret)
        now = int(time.time())
        payload = {
            "iss": "cdp",
            "sub": api_key,
            "nbf": now,
            "exp": now + 120,
            # Coinbase examples use host-less URI for the verify endpoint.
            "uri": "GET /users/self/verify",
        }
        headers = {"kid": api_key, "nonce": uuid.uuid4().hex}
        for secret in secrets:
            try:
                key_obj = serialization.load_pem_private_key(secret.encode(), password=None, backend=default_backend())
                return jwt.encode(payload, key_obj, algorithm="ES256", headers=headers)
            except Exception:
                continue
        log.warning("Failed to build Coinbase ADV JWT: unable to parse provided private key")
        return None

    @staticmethod
    def _wrap_pem(body: str, header: str) -> str:
        wrapped = "\n".join(textwrap.wrap(body.replace("\n", ""), 64))
        return f"-----BEGIN {header}-----\n{wrapped}\n-----END {header}-----"

    def _load_secret_candidates(self, raw: str) -> list[str]:
        # Accept raw string, JSON file with {"privateKey": ...}, or PEM file path.
        try:
            if os.path.exists(raw):
                if raw.lower().endswith(".json"):
                    import json

                    with open(raw, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                        raw = data.get("privateKey", raw)
                else:
                    with open(raw, "r", encoding="utf-8") as fh:
                        raw = fh.read()
        except Exception:
            pass
        raw = (raw or "").strip()
        raw = raw.replace("\r\n", "\n").replace("\\n", "\n")

        candidates: list[str] = []
        if raw.startswith("-----BEGIN"):
            candidates.append(raw)
        else:
            candidates.append(self._wrap_pem(raw, "PRIVATE KEY"))       # PKCS8
            candidates.append(self._wrap_pem(raw, "EC PRIVATE KEY"))    # SEC1

        # Attempt to normalize via cryptography to PKCS8
        normalized: list[str] = []
        for pem in candidates:
            try:
                key = serialization.load_pem_private_key(pem.encode(), password=None, backend=default_backend())
                pkcs8 = key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                normalized.append(pkcs8.decode())
            except Exception:
                continue
        return normalized + candidates

    @staticmethod
    def _to_product_id(symbol: str) -> str:
        s = symbol.strip().upper()
        if "/" in s:
            base, quote = s.split("/", 1)
        elif s.endswith("USDT"):
            base, quote = s[:-4], s[-4:]
        elif s.endswith("USDC"):
            base, quote = s[:-4], s[-4:]
        elif s.endswith("USD"):
            base, quote = s[:-3], s[-3:]
        elif s.endswith("BTC"):
            base, quote = s[:-3], s[-3:]
        elif s.endswith("ETH"):
            base, quote = s[:-3], s[-3:]
        else:
            base, quote = s[:-3], s[-3:]
        if not base or not quote:
            raise ValueError(f"Unable to convert {symbol} to Coinbase product id")
        return f"{base}-{quote}"

    async def handle_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            metrics.WS_MESSAGE_ERRORS.labels(exchange="COINBASE_ADV").inc()
            return
        channel = payload.get("channel")
        if channel == "l2_data":
            await self._handle_l2(payload)
        elif channel == "ticker":
            await self._handle_ticker(payload)
        elif payload.get("type") == "error":
            log.warning("Coinbase ADV WS error: %s", payload)
            metrics.WS_MESSAGE_ERRORS.labels(exchange="COINBASE_ADV").inc()

    async def _handle_l2(self, payload: dict) -> None:
        if self._on_depth is None:
            return
        events = payload.get("events") or []
        for evt in events:
            product = evt.get("product_id")
            if not product or "/" in product:
                continue
            symbol = product.replace("-", "").upper()
            builder = self._depth_books.setdefault(symbol, DepthBook("COINBASE", symbol, depth=self._depth_levels))
            updates = evt.get("updates") or []
            ts_event = _parse_ts(evt.get("timestamp"))
            if evt.get("type") == "snapshot":
                bids = []
                asks = []
                for u in updates:
                    side = u.get("side")
                    try:
                        price = float(u.get("price_level"))
                        qty = float(u.get("new_quantity"))
                    except (TypeError, ValueError):
                        continue
                    if side == "bid":
                        bids.append((price, qty))
                    elif side == "ask" or side == "offer":
                        asks.append((price, qty))
                builder.snapshot(bids=bids, asks=asks, ts_event=ts_event)
                metrics.WS_DEPTH_RESETS.labels(exchange="COINBASE").inc()
            else:  # update
                changes = []
                for u in updates:
                    side = u.get("side")
                    try:
                        price = float(u.get("price_level"))
                        qty = float(u.get("new_quantity"))
                    except (TypeError, ValueError):
                        continue
                    side_norm = "bid" if side == "bid" else "ask"
                    changes.append((side_norm, price, qty))
                if changes:
                    builder.update(changes, ts_event)
            snapshot = builder.to_snapshot(source="l2_data")
            metrics.WS_DEPTH_UPDATES.labels(exchange="COINBASE", kind="l2").inc()
            metrics.WS_LAST_DEPTH_TS.labels(exchange="COINBASE", symbol=symbol).set(ts_event)
            await self._on_depth(snapshot)

    async def _handle_ticker(self, payload: dict) -> None:
        if self._on_ticker is None:
            return
        events = payload.get("events") or []
        for evt in events:
            if evt.get("type") != "ticker":
                continue
            product = evt.get("product_id")
            if not product:
                continue
            symbol = product.replace("-", "").upper()
            price = evt.get("price")
            if price is None:
                continue
            try:
                price_f = float(price)
                best_bid = float(evt.get("best_bid") or price_f)
                best_ask = float(evt.get("best_ask") or price_f)
                size = float(evt.get("last_size") or 0.0)
            except (TypeError, ValueError):
                metrics.WS_MESSAGE_ERRORS.labels(exchange="COINBASE_ADV").inc()
                continue
            ticker = CoinbaseTicker(
                product_id=product,
                symbol=symbol,
                price=price_f,
                size=size,
                side=(evt.get("side") or "UNKNOWN").upper(),
                ts_event=_parse_ts(evt.get("time")),
                best_bid=best_bid,
                best_ask=best_ask,
                best_bid_size=size,
                best_ask_size=size,
            )
            await self._on_ticker(ticker)
