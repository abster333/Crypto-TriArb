from __future__ import annotations

import asyncio
import logging
import os
from typing import List
from pathlib import Path

from cex_triarb_v1.ingest.symbols import discover_per_exchange
from cex_triarb_v1.strategy.auto_build import generate_cycles_for_exchange
from cex_depth_v1.ingest_service import DepthIngestService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("depth-ingest")


def env_list(key: str, default: str = "") -> List[str]:
    raw = os.getenv(key, default)
    return [t.strip() for t in raw.split(",") if t.strip()]


def _load_env() -> None:
    """Best-effort load of .env.depth or .env into os.environ (if not already set)."""
    for candidate in (".env.depth", ".env"):
        path = Path(candidate)
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            os.environ.setdefault(key, val)
        log.info("Loaded environment from %s", path)
        break


async def amain() -> None:
    _load_env()
    symbols = env_list("DEPTH_SYMBOLS")
    exchanges = env_list("DEPTH_EXCHANGES", "COINBASE,KRAKEN")
    nats_url = os.getenv("DEPTH_NATS_URL", "nats://127.0.0.1:4222")
    redis_url = os.getenv("DEPTH_REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = os.getenv("DEPTH_REDIS_PREFIX", "md_depth")
    snapshot_subject = os.getenv("DEPTH_SNAPSHOT_SUBJECT", "md.depth")
    health_subject = os.getenv("DEPTH_HEALTH_SUBJECT", "md.depth.health")
    control_subject = os.getenv("DEPTH_CONTROL_SUBJECT", "strategy.depth.ctrl.symbols")
    health_port = int(os.getenv("DEPTH_HEALTH_PORT", "8087"))
    depth_levels = int(os.getenv("DEPTH_LEVELS", "10"))
    # Depth-only stack: default to level2 only to avoid wasting Coinbase ticker slots.
    cb_channels_env = os.getenv("DEPTH_CB_CHANNELS", "level2")
    cb_channels = [c.strip() for c in cb_channels_env.split(",") if c.strip()]
    kraken_book_depth = int(os.getenv("DEPTH_KRAKEN_BOOK_DEPTH", str(max(10, depth_levels))))
    kraken_batch_size = int(os.getenv("DEPTH_KRAKEN_BATCH_SIZE", "100"))
    okx_book_depth = int(os.getenv("DEPTH_OKX_BOOK_DEPTH", str(depth_levels)))
    cb_ws_mode = os.getenv("COINBASE_WS_MODE", os.getenv("DEPTH_CB_WS_MODE", "ADVANCED"))
    cb_ws_batch = int(os.getenv("COINBASE_WS_BATCH", os.getenv("DEPTH_CB_WS_BATCH", "40")))
    cb_api_key = os.getenv("COINBASE_API_KEY")
    cb_api_secret = os.getenv("COINBASE_API_SECRET")
    cb_api_passphrase = os.getenv("COINBASE_API_PASSPHRASE")

    per_exchange_symbols = {}
    if not symbols:
        log.info("No DEPTH_SYMBOLS provided; discovering tradable pairs for %s", exchanges)
        symbols, per_exchange_symbols = await discover_per_exchange(exchanges)
    else:
        for ex in exchanges:
            per_exchange_symbols.setdefault(ex.upper(), [s.upper() for s in symbols])

    service = DepthIngestService(
        symbols=symbols,
        exchanges=exchanges,
        nats_url=nats_url,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
        snapshot_subject=snapshot_subject,
        health_subject=health_subject,
        control_subject=control_subject,
        rest_interval_sec=10,
        health_port=health_port,
        rest_enabled=False,
        per_exchange_symbols=per_exchange_symbols or None,
        depth_enabled=True,
        depth_levels=depth_levels,
        coinbase_channels=cb_channels,
        kraken_book_depth=kraken_book_depth,
        kraken_batch_size=kraken_batch_size,
        coinbase_ws_mode=cb_ws_mode,
        coinbase_batch_size=cb_ws_batch,
        coinbase_api_key=cb_api_key,
        coinbase_api_secret=cb_api_secret,
        coinbase_api_passphrase=cb_api_passphrase,
        okx_book_depth=okx_book_depth,
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

    log.info("Depth IngestService running; ctrl+c to exit")
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
