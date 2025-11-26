"""Simple async circuit breaker."""

from __future__ import annotations

import time
from enum import Enum
from typing import Awaitable, Callable, TypeVar

from v5.common import metrics

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure = 0.0

    async def call(self, fn: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure > self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise RuntimeError("circuit_open")
        try:
            result = await fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED
        try:
            metrics.CIRCUIT_STATE.labels(component="rpc").set(0)
        except Exception:
            pass

    def _on_failure(self) -> None:
        self.failures += 1
        self.last_failure = time.time()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
        state_val = 2 if self.state == CircuitState.OPEN else 1
        try:
            metrics.CIRCUIT_STATE.labels(component="rpc").set(state_val)
        except Exception:
            pass


__all__ = ["CircuitBreaker", "CircuitState"]
