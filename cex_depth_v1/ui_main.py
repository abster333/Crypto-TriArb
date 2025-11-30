from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

import uvicorn

from cex_depth_v1.ui.server import DepthDashboardService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("depth-ui-main")


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
    nats_url = os.getenv("DEPTH_NATS_URL", "nats://127.0.0.1:4222")
    snapshot_subject = os.getenv("DEPTH_SNAPSHOT_SUBJECT", "md.depth")
    symbols_env = os.getenv("DEPTH_SYMBOLS")
    symbols = [s.strip() for s in symbols_env.split(",")] if symbols_env else None
    host = os.getenv("DEPTH_UI_HOST", "0.0.0.0")
    port = int(os.getenv("DEPTH_UI_PORT", "8090"))
    cycles_subject = os.getenv("STRAT_CYCLES_SUBJECT", "strategy.depth.cycles")
    redis_url = os.getenv("DEPTH_REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = os.getenv("DEPTH_REDIS_PREFIX", "md_depth")

    dashboard = DepthDashboardService(
        nats_url=nats_url,
        snapshot_subject=snapshot_subject,
        symbols=symbols,
        cycles_subject=cycles_subject,
        redis_url=redis_url,
        redis_prefix=redis_prefix,
    )
    await dashboard.start()
    config = uvicorn.Config(dashboard.app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    log.info("Depth dashboard listening on %s:%s (NATS %s)", host, port, nats_url)
    try:
        await server.serve()
    finally:
        await dashboard.stop()


def main() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except Exception:
        pass
    asyncio.run(amain())


if __name__ == "__main__":
    main()
