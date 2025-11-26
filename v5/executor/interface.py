"""Execution interface for future order placement."""

from __future__ import annotations

import abc

from v5.common.models import Opportunity


class Executor(abc.ABC):
    @abc.abstractmethod
    async def execute(self, opportunity: Opportunity) -> dict:
        """Execute the arbitrage opportunity and return a result payload."""
        raise NotImplementedError
