from __future__ import annotations

import asyncio
import logging
import os
from typing import List

from cex_triarb_v1.ingest.runner import IngestService
from cex_triarb_v1.ingest.symbols import discover_symbols, discover_per_exchange
from cex_triarb_v1.strategy.auto_build import generate_cycles_for_exchange

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("main")


def env_list(key: str, default: str = "") -> List[str]:
    raw = os.getenv(key, default)
    return [t.strip() for t in raw.split(",") if t.strip()]


async def amain() -> None:
    symbols = env_list("SYMBOLS")
    exchanges = env_list("EXCHANGES", "COINBASE,KRAKEN")
    nats_url = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = os.getenv("REDIS_PREFIX", "md")
    snapshot_subject = os.getenv("SNAPSHOT_SUBJECT", "md.snapshot")
    health_subject = os.getenv("HEALTH_SUBJECT", "md.health")
    control_subject = os.getenv("CONTROL_SUBJECT", "strategy.ctrl.symbols")
    rest_interval = int(os.getenv("REST_INTERVAL_SEC", "10"))
    health_port = int(os.getenv("HEALTH_PORT", "8085"))
    rest_enabled = os.getenv("REST_ENABLED", "false").lower() in ("1", "true", "yes")
    depth_enabled = os.getenv("DEPTH_ENABLED", "false").lower() in ("1", "true", "yes")
    depth_levels = int(os.getenv("DEPTH_LEVELS", "10"))
    cb_channels_env = os.getenv("CB_WS_CHANNELS")
    cb_channels = [c.strip() for c in cb_channels_env.split(",") if c.strip()] if cb_channels_env else (["ticker", "level2"] if depth_enabled else ["ticker"])
    kraken_book_depth = int(os.getenv("KRAKEN_BOOK_DEPTH", str(max(10, depth_levels))))
    kraken_batch_size = int(os.getenv("KRAKEN_BATCH_SIZE", "100"))
    cb_ws_mode = os.getenv("COINBASE_WS_MODE", "EXCHANGE")
    cb_ws_batch = int(os.getenv("COINBASE_WS_BATCH", "40"))
    cb_api_key = os.getenv("COINBASE_API_KEY")
    cb_api_secret = os.getenv("COINBASE_API_SECRET")
    cb_api_passphrase = os.getenv("COINBASE_API_PASSPHRASE")

    per_exchange_symbols = {}
    # Auto-discover symbols when none provided.
    if not symbols:
        log.info("No SYMBOLS provided; discovering all tradable pairs for %s", exchanges)
        symbols, per_exchange_symbols = await discover_per_exchange(exchanges)
    else:
        # If symbols are provided, map per-exchange assuming availability across all specified exchanges.
        for ex in exchanges:
            per_exchange_symbols.setdefault(ex.upper(), [s.upper() for s in symbols])

    service = IngestService(
        symbols=symbols,
        exchanges=exchanges,
        nats_url=nats_url,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
        snapshot_subject=snapshot_subject,
        health_subject=health_subject,
        control_subject=control_subject,
        rest_interval_sec=rest_interval,
        health_port=health_port,
        rest_enabled=rest_enabled,
        per_exchange_symbols=per_exchange_symbols or None,
        depth_enabled=depth_enabled,
        depth_levels=depth_levels,
        coinbase_channels=cb_channels,
        kraken_book_depth=kraken_book_depth,
        kraken_batch_size=kraken_batch_size,
        coinbase_ws_mode=cb_ws_mode,
        coinbase_batch_size=cb_ws_batch,
        coinbase_api_key=cb_api_key,
        coinbase_api_secret=cb_api_secret,
        coinbase_api_passphrase=cb_api_passphrase,
    )
    await service.start()

    stop_event = asyncio.Event()

    def _stop(*_args) -> None:
        stop_event.set()

    for sig in ("SIGINT", "SIGTERM"):
        try:
            asyncio.get_running_loop().add_signal_handler(getattr(__import__("signal"), sig), _stop)
        except (AttributeError, RuntimeError, ValueError):
            pass

    log.info("IngestService running; ctrl+c to exit")
    await stop_event.wait()
    await service.stop()


def main() -> None:
    try:
        import uvloop

        uvloop.install()
    except Exception:
        pass
    asyncio.run(amain())


if __name__ == "__main__":
    main()
