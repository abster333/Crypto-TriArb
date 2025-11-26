from __future__ import annotations

import asyncio
import logging
import os
import json
from pathlib import Path
import nats
import redis.asyncio as redis  # type: ignore

from cex_triarb_v1.ingest.symbols import discover_per_exchange
from cex_triarb_v1.strategy.auto_build import generate_cycles_for_exchange
from cex_triarb_v1.strategy.consumer import parse_cycles, run_strategy
from cex_triarb_v1.strategy.fees import fee_map

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("strategy-main")


async def amain() -> None:
    cycles_env = os.getenv("STRAT_CYCLES", "")
    exchanges = [e.strip() for e in os.getenv("STRAT_EXCHANGES", "COINBASE,KRAKEN").split(",") if e.strip()]
    cycles = parse_cycles(cycles_env)
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

    to_log = cycles if log_all_cycles else cycles[:20]
    for i, c in enumerate(to_log):
        legs_fmt = ",".join(f"{l.exchange}:{l.symbol}:{l.side}" for l in c.legs)
        log.info("Cycle[%d]: %s", i, legs_fmt)

    await publish_cycles_once(cycles, cycles_subject, nats_url, redis_url, redis_prefix)
    nats_url = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    snapshot_subject = os.getenv("SNAPSHOT_SUBJECT", "md.snapshot")
    snapshot_subject = os.getenv("STRAT_SNAPSHOT_SUBJECT", snapshot_subject)
    roi_bps = float(os.getenv("STRAT_ROI_BPS", "5.0"))
    fees = fee_map(exchanges)
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = os.getenv("REDIS_PREFIX", "md")
    use_depth = os.getenv("STRAT_USE_DEPTH", "false").lower() in ("1", "true", "yes")
    debug_depth = os.getenv("STRAT_DEBUG_DEPTH", "false").lower() in ("1", "true", "yes")
    debug_depth_verbose = os.getenv("STRAT_DEBUG_DEPTH_VERBOSE", "false").lower() in ("1", "true", "yes")
    cycles_subject = os.getenv("STRAT_CYCLES_SUBJECT", "strategy.cycles")
    start_notional_raw = os.getenv("STRAT_START_NOTIONAL")
    start_notional = float(start_notional_raw) if start_notional_raw not in (None, "") else float("inf")
    await run_strategy(
        cycles,
        nats_url,
        snapshot_subject,
        roi_bps,
        fees_bps=fees,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
        use_depth=use_depth,
        debug_depth=debug_depth,
        debug_depth_verbose=debug_depth_verbose,
        start_notional=start_notional,
    )


def main() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except Exception:
        pass
    asyncio.run(amain())


if __name__ == "__main__":
    main()


async def publish_cycles_once(cycles, subject: str, nats_url: str, redis_url: str, redis_prefix: str) -> None:
    payload = json.dumps([{"id": c.id, "legs": [l.__dict__ for l in c.legs]} for c in cycles])
    try:
        nc = await nats.connect(nats_url)
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
