"""Stub executor that records opportunities without trading."""

from __future__ import annotations

import logging

from v5.common.models import Opportunity
from v5.executor.interface import Executor

log = logging.getLogger(__name__)


class NullExecutor(Executor):
    async def execute(self, opportunity: Opportunity) -> dict:
        log.info("Received opportunity roi=%.5f", opportunity.total_roi)
        return {"status": "noop", "roi": opportunity.total_roi}
