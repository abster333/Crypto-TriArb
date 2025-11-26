from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class WsEvent:
    pool: str
    block_number: Optional[int]
    ts_ms: int
    sqrt_price_x96: int
    liquidity: int
    tick: int


__all__ = ["WsEvent"]
