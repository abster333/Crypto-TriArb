from __future__ import annotations

import asyncio
import logging
import os
import json
from pathlib import Path
from typing import List

import nats
import redis.asyncio as redis  # type: ignore

from cex_triarb_v1.ingest.symbols import discover_per_exchange
from cex_triarb_v1.ingest.publishers import connect_nats_with_retry
from cex_triarb_v1.strategy.auto_build import generate_cycles_for_exchange
from cex_triarb_v1.strategy.consumer import StrategyConsumer, parse_cycles
from cex_triarb_v1.strategy.fees import fee_map
from cex_triarb_v1.persistence.ndjson_logger import NdjsonLogger

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("depth-strategy-main")


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
    cycles_env = os.getenv("DEPTH_STRAT_CYCLES", "")
    exchanges_raw = os.getenv("DEPTH_STRAT_EXCHANGES") or os.getenv("DEPTH_EXCHANGES")
    if exchanges_raw:
        log.info("Using exchanges from env (DEPTH_STRAT_EXCHANGES/DEPTH_EXCHANGES): %s", exchanges_raw)
    else:
        exchanges_raw = "COINBASE"  # safer default than previous KRAKEN
        log.info("No exchanges env provided; defaulting to %s", exchanges_raw)
    exchanges = [e.strip() for e in exchanges_raw.split(",") if e.strip()]
    cycles = parse_cycles(cycles_env)
    strat_symbols = [s.replace("/", "").replace("-", "").replace("_", "").upper() for s in (env_list("STRAT_SYMBOLS") or env_list("DEPTH_SYMBOLS"))] if (env_list("STRAT_SYMBOLS") or env_list("DEPTH_SYMBOLS")) else None

    # Shared cache locations are needed before discovery so redis-backed desired sets are honored on restart.
    redis_url = os.getenv("DEPTH_REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = os.getenv("DEPTH_REDIS_PREFIX", "md_depth")

    if not cycles:
        # Prefer ingest-advertised symbols from Redis desired set for consistency with ingest.
        per_exchange = {}
        try:
            from cex_triarb_v1.ingest.publishers import RedisHotCache

            r = RedisHotCache(url=redis_url, prefix=redis_prefix)
            await r.start()
            _, per_exchange = await r.load_desired_symbols()
            await r.stop()
        except Exception:
            per_exchange = {}
        if not per_exchange:
            _, per_exchange = await discover_per_exchange(exchanges)
        all_cycles = []
        for ex, symbols in per_exchange.items():
            c = generate_cycles_for_exchange(ex, set(symbols))
            all_cycles.extend(c)
        cycles = all_cycles
    log.info("Auto-built %d cycles across %s", len(cycles), exchanges)

    # Enforce exchange allowlist in case discovery returned extras.
    allowed_exchanges = {ex.upper() for ex in exchanges}
    before_cycles = len(cycles)
    cycles = [c for c in cycles if all(leg.exchange.upper() in allowed_exchanges for leg in c.legs)]
    if len(cycles) != before_cycles:
        log.info("Filtered cycles by DEPTH_STRAT_EXCHANGES/DEPTH_EXCHANGES: %d -> %d", before_cycles, len(cycles))

    log_all_cycles = os.getenv("STRAT_LOG_ALL_CYCLES", "false").lower() in ("1", "true", "yes")
    dump_cycles_path = os.getenv("STRAT_CYCLE_DUMP")

    if dump_cycles_path:
        try:
            Path(dump_cycles_path).write_text(
                json.dumps(
                    [{"id": c.id, "legs": [l.__dict__ for l in c.legs]} for c in cycles],
                    indent=2,
                ),
                encoding="utf-8",
            )
            log.info("Cycle list written to %s", dump_cycles_path)
        except Exception as exc:
            log.warning("Failed to write cycle dump to %s: %s", dump_cycles_path, exc)

    if strat_symbols:
        allowed = {s.upper() for s in strat_symbols}
        before = len(cycles)
        cycles = [c for c in cycles if all(leg.symbol in allowed for leg in c.legs)]
        log.info("Filtered cycles by STRAT_SYMBOLS/DEPTH_SYMBOLS: %d -> %d (allowed=%d symbols)", before, len(cycles), len(allowed))
    nats_url = os.getenv("DEPTH_NATS_URL", "nats://127.0.0.1:4222")
    snapshot_subject = os.getenv("STRAT_SNAPSHOT_SUBJECT", os.getenv("DEPTH_SNAPSHOT_SUBJECT", "md.depth"))
    roi_bps = float(os.getenv("DEPTH_ROI_BPS", os.getenv("STRAT_ROI_BPS", "5.0")))
    fees = fee_map(exchanges)
    publish_subject = os.getenv("DEPTH_STRAT_PUBLISH_SUBJECT", "strategy.depth.opportunity")
    cycles_subject = os.getenv("STRAT_CYCLES_SUBJECT", "strategy.depth.cycles")
    log_path = os.getenv("DEPTH_OPP_LOG", "logs/depth_opps.ndjson")
    use_depth = os.getenv("STRAT_USE_DEPTH", "true").lower() in ("1", "true", "yes")
    debug_depth = os.getenv("STRAT_DEBUG_DEPTH", "false").lower() in ("1", "true", "yes")
    debug_depth_verbose = os.getenv("STRAT_DEBUG_DEPTH_VERBOSE", "false").lower() in ("1", "true", "yes")
    start_notional_raw = os.getenv("DEPTH_START_NOTIONAL", os.getenv("STRAT_START_NOTIONAL"))
    start_notional = float(start_notional_raw) if start_notional_raw not in (None, "") else float("inf")

    # Publish cycles to Redis/NATS for UI/consumers.
    await publish_cycles_once(cycles, cycles_subject, nats_url, redis_url, redis_prefix)

    # Elevate logging when debug flag is set so debug-level cycle traces are visible.
    if debug_depth:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("cex_triarb_v1.strategy").setLevel(logging.DEBUG)

    consumer = StrategyConsumer(
        cycles=cycles,
        nats_url=nats_url,
        snapshot_subject=snapshot_subject,
        roi_bps=roi_bps,
        fees_bps=fees,
        logger=NdjsonLogger(log_path),
        publish_subject=publish_subject,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
        use_depth=use_depth,
        debug_depth=debug_depth,
        debug_depth_verbose=debug_depth_verbose,
        start_notional=start_notional,
    )
    await consumer.start()
    stop_event = asyncio.Event()

    def _stop(*_args) -> None:
        stop_event.set()

    try:
        import signal

        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
    except Exception:
        pass

    log.info("Depth strategy running; ctrl+c to exit")
    await stop_event.wait()
    await consumer.stop()


async def publish_cycles_once(cycles, subject: str, nats_url: str, redis_url: str, redis_prefix: str) -> None:
    payload = json.dumps([{"id": c.id, "legs": [l.__dict__ for l in c.legs]} for c in cycles])
    try:
        nc = await connect_nats_with_retry(nats_url)
        await nc.publish(subject, payload.encode())
        await nc.drain()
    except Exception as exc:
        log.warning("Cycle publish to NATS failed: %s", exc)
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        await r.set(f"{redis_prefix}:cycles", payload)
        await r.aclose()
    except Exception as exc:
        log.warning("Cycle publish to Redis failed: %s", exc)


def main() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except Exception:
        pass
    asyncio.run(amain())


if __name__ == "__main__":
    main()
