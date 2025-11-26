from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Iterable, List, Sequence


@dataclass
class NormalizedBook:
    exchange: str
    symbol: str
    bids: List[tuple[float, float]]
    asks: List[tuple[float, float]]
    ts_event: int


@dataclass
class NormalizedTrade:
    exchange: str
    symbol: str
    price: float
    size: float
    side: str
    ts_event: int


class BaseWsAdapter(abc.ABC):
    """Base class for websocket adapters."""

    def __init__(self, exchange: str, symbols: Sequence[str]) -> None:
        self.exchange = exchange.upper()
        self.symbols: List[str] = [s.upper() for s in symbols]

    @abc.abstractmethod
    async def run_forever(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def tracked_symbols(self) -> Iterable[str]:
        return tuple(self.symbols)
