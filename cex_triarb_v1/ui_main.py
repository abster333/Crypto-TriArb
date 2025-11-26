from __future__ import annotations

import asyncio
import logging
import os

import uvicorn

from cex_triarb_v1.ui.server import DashboardService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("ui-main")


async def amain() -> None:
    nats_url = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    snapshot_subject = os.getenv("SNAPSHOT_SUBJECT", "md.snapshot")
    symbols_env = os.getenv("SYMBOLS")
    symbols = [s.strip() for s in symbols_env.split(",")] if symbols_env else None
    host = os.getenv("UI_HOST", "0.0.0.0")
    port = int(os.getenv("UI_PORT", "8086"))

    dashboard = DashboardService(nats_url=nats_url, snapshot_subject=snapshot_subject, symbols=symbols)
    await dashboard.start()
    config = uvicorn.Config(dashboard.app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    log.info("Dashboard listening on %s:%s (NATS %s)", host, port, nats_url)
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
