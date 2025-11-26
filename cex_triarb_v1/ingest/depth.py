from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Sequence, Tuple

DepthRow = Tuple[float, float, float]


@dataclass
class DepthSnapshot:
    exchange: str
    symbol: str
    ts_event: int
    bids: List[DepthRow]
    asks: List[DepthRow]
    depth: int
    sequence: int | None = None
    source: str | None = None


class _DepthSide:
    def __init__(self, depth: int, descending: bool) -> None:
        self.depth = depth
        self.descending = descending
        self._levels: Dict[float, float] = {}

    def reset(self, levels: Iterable[Sequence[float]]) -> None:
        self._levels.clear()
        for price, size in levels:
            price_f = float(price)
            size_f = float(size)
            if size_f <= 0:
                continue
            self._levels[price_f] = size_f

    def apply(self, price: float, size: float) -> None:
        price_f = float(price)
        size_f = float(size)
        if size_f <= 0:
            self._levels.pop(price_f, None)
            return
        self._levels[price_f] = size_f

    def top(self) -> List[DepthRow]:
        ordered = sorted(self._levels.items(), key=lambda item: item[0], reverse=self.descending)
        rows: List[DepthRow] = []
        cumulative = 0.0
        for price, size in ordered:
            cumulative += size
            rows.append((price, size, cumulative))
            if len(rows) >= self.depth:
                break
        return rows


class DepthBook:
    def __init__(self, exchange: str, symbol: str, depth: int = 5) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.depth = depth
        self.sequence: int | None = None
        self.ts_event: int = 0
        self.bids = _DepthSide(depth, descending=True)
        self.asks = _DepthSide(depth, descending=False)

    def snapshot(self, bids: Iterable[Sequence[float]], asks: Iterable[Sequence[float]], ts_event: int, sequence: int | None = None) -> None:
        self.bids.reset(bids)
        self.asks.reset(asks)
        self.sequence = sequence
        self.ts_event = ts_event

    def update(self, changes: Iterable[tuple[Literal["bid", "ask"], float, float]], ts_event: int, sequence: int | None = None) -> None:
        for side, price, size in changes:
            if side == "bid":
                self.bids.apply(price, size)
            else:
                self.asks.apply(price, size)
        self.sequence = sequence
        self.ts_event = ts_event

    def to_snapshot(self, source: str | None = None) -> DepthSnapshot:
        return DepthSnapshot(
            exchange=self.exchange,
            symbol=self.symbol,
            ts_event=self.ts_event,
            bids=self.bids.top(),
            asks=self.asks.top(),
            depth=self.depth,
            sequence=self.sequence,
            source=source,
        )
