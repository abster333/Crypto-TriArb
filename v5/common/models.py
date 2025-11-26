"""Shared, strongly validated data models for V5.

These Pydantic models define the contracts between modules. Validation is
strict and fails fast to keep bad data from propagating into pricing or
execution logic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


def _is_hex_address(value: str) -> bool:
    """Return True if the string looks like a 20-byte hex address."""
    if not isinstance(value, str):
        return False
    if not value.startswith("0x") or len(value) != 42:
        return False
    try:
        int(value[2:], 16)
    except ValueError:
        return False
    return True


class Token(BaseModel):
    """Canonical token metadata."""

    address: str = Field(..., description="EVM address, 0x-prefixed 40 hex chars")
    symbol: str = Field(..., description="Uppercase token symbol, e.g. WETH")
    decimals: int = Field(..., ge=0, le=36, description="Token decimals")
    chain_id: int = Field(..., ge=1, description="EVM chain id (e.g. 1 for mainnet)")

    @field_validator("address")
    @classmethod
    def _valid_address(cls, v: str) -> str:
        if not _is_hex_address(v):
            raise ValueError("address must be 0x-prefixed 40 hex chars")
        return v.lower()

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        if not v or not v.isascii():
            raise ValueError("symbol required and must be ASCII")
        return v.upper()

    def __repr__(self) -> str:  # pragma: no cover - convenience
        return f"Token(symbol={self.symbol}, addr={self.address}, chain={self.chain_id})"


class PoolMeta(BaseModel):
    """Static pool configuration loaded from manifest."""

    pool_address: str = Field(..., description="Pool contract address")
    platform: str = Field(..., description="DEX platform id, e.g. UNISWAP_V3")
    token0: str
    token1: str
    fee_tier: int = Field(..., ge=0, description="Fee in ppm or bps depending on platform")
    network: str = Field(..., description="Network key, e.g. eth, bsc")
    dec0: int = Field(18, description="Decimals token0")
    dec1: int = Field(18, description="Decimals token1")

    @field_validator("pool_address")
    @classmethod
    def _valid_pool_address(cls, v: str) -> str:
        if not _is_hex_address(v):
            raise ValueError("pool_address must be 0x-prefixed 40 hex chars")
        return v.lower()

    @field_validator("platform", "network")
    @classmethod
    def _uppercase(cls, v: str) -> str:
        if not v or not v.isascii():
            raise ValueError("value must be ASCII and non-empty")
        return v.upper()

    @field_validator("token0", "token1")
    @classmethod
    def _token_symbol(cls, v: str) -> str:
        if not v or not v.isascii():
            raise ValueError("token symbols must be ASCII")
        return v.upper()

    def __repr__(self) -> str:  # pragma: no cover
        return f"PoolMeta({self.platform}@{self.network}:{self.pool_address})"


class DepthLevels(BaseModel):
    """Structured order book depth."""

    bids: List[Tuple[float, float]] = Field(default_factory=list, description="(price, qty)")
    asks: List[Tuple[float, float]] = Field(default_factory=list, description="(price, qty)")

    @field_validator("bids", "asks")
    @classmethod
    def _positive_levels(cls, v: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        for price, qty in v:
            if price <= 0 or qty <= 0:
                raise ValueError("price and qty must be positive")
        return v

    def __repr__(self) -> str:  # pragma: no cover
        return f"DepthLevels(bids={len(self.bids)}, asks={len(self.asks)})"


class PoolState(BaseModel):
    """Latest known on-chain state for a pool."""

    pool_meta: PoolMeta
    sqrt_price_x96: str = Field(..., description="Raw slot0 sqrtPriceX96 value (hex or int string)")
    tick: int
    liquidity: str = Field(..., description="Raw liquidity hex/int string")
    price: float | None = None  # normalized token1 per token0
    liquidity_int: int | None = None  # normalized int liquidity (raw converted to int)
    block_number: int = Field(..., ge=0)
    timestamp_ms: int = Field(..., ge=0)
    last_snapshot_ms: Optional[int] = None
    last_ws_refresh_ms: Optional[int] = None
    stream_ready: bool = False

    @field_validator("sqrt_price_x96", "liquidity")
    @classmethod
    def _hex_or_int(cls, v: str) -> str:
        if isinstance(v, int):
            return str(v)
        sv = str(v)
        if sv.startswith("0x"):
            int(sv, 16)
        else:
            int(sv)
        return sv

    def __repr__(self) -> str:  # pragma: no cover
        return f"PoolState({self.pool_meta.pool_address} tick={self.tick})"


class OpportunityLeg(BaseModel):
    """Single leg of an arbitrage cycle."""

    pool: str
    token_in: str
    token_out: str
    amount_in: float = Field(..., gt=0)
    amount_out: float = Field(..., gt=0)
    price_impact: float = Field(..., ge=0)

    @field_validator("pool")
    @classmethod
    def _pool_addr(cls, v: str) -> str:
        if not _is_hex_address(v):
            raise ValueError("pool must be valid address")
        return v.lower()

    def __repr__(self) -> str:  # pragma: no cover
        return f"Leg({self.token_in}->{self.token_out} in={self.amount_in} out={self.amount_out})"


class Opportunity(BaseModel):
    """Validated arbitrage opportunity emitted by simulator."""

    legs: List[OpportunityLeg]
    total_roi: float
    execution_cost: float = Field(..., ge=0)
    confidence: float = Field(..., ge=0, le=1)
    timestamp_ms: int = Field(..., ge=0, description="Detection time in ms since epoch")
    profit: float | None = None
    notional: float | None = None
    profit_usd: float | None = None
    size_cap: float | None = Field(
        default=None,
        description="Estimated max starting-token amount before price impact ~= ROI",
        ge=0,
    )

    @model_validator(mode="after")
    def _roi_non_negative(self) -> "Opportunity":
        if self.total_roi < -1:  # allow small negatives but not nonsense
            raise ValueError("total_roi too negative")
        return self

    def __repr__(self) -> str:  # pragma: no cover
        return f"Opportunity(roi={self.total_roi:.6f}, legs={len(self.legs)})"


class HealthStatus(BaseModel):
    """Per-pool health snapshot for visibility/alarms."""

    pool_address: str
    is_stale: bool
    last_snapshot_ms: Optional[int] = None
    last_ws_ms: Optional[int] = None
    has_data: bool = True

    @field_validator("pool_address")
    @classmethod
    def _pool_addr(cls, v: str) -> str:
        if not _is_hex_address(v):
            raise ValueError("pool_address must be valid address")
        return v.lower()

    @field_validator("last_snapshot_ms", "last_ws_ms")
    @classmethod
    def _ts_non_negative(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("timestamp must be non-negative")
        return v

    def __repr__(self) -> str:  # pragma: no cover
        return f"Health(pool={self.pool_address}, stale={self.is_stale})"


class AlarmConfig(BaseModel):
    """Alarm thresholds and destinations."""

    stale_snapshot_ms: int = Field(300_000, ge=0)
    stale_ws_ms: int = Field(60_000, ge=0)
    missing_data_threshold: int = Field(0, ge=0)
    webhook_urls: List[HttpUrl] = Field(default_factory=list)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"AlarmConfig(stale_snapshot_ms={self.stale_snapshot_ms}, "
            f"stale_ws_ms={self.stale_ws_ms}, webhooks={len(self.webhook_urls)})"
        )


__all__ = [
    "Token",
    "PoolMeta",
    "PoolState",
    "DepthLevels",
    "OpportunityLeg",
    "Opportunity",
    "HealthStatus",
    "AlarmConfig",
]
