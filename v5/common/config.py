"""Configuration loading and manifest validation utilities."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from jsonschema import Draft7Validator
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from v5.common.models import AlarmConfig, PoolMeta, Token
from v5.manifest_generator_async import build_manifests_async, parse_targets


def _load_json(path: str | Path) -> Dict[str, Any]:
    with Path(path).expanduser().open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_schema(name: str) -> Dict[str, Any]:
    here = Path(__file__).resolve().parent / "schemas"
    return _load_json(here / name)


def validate_json_manifest(payload: Dict[str, Any], schema_name: str) -> None:
    """Validate a manifest dict against a bundled JSON schema."""
    schema = _load_schema(schema_name)
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        msgs = "; ".join(f"{'/'.join(map(str, e.path))}: {e.message}" for e in errors)
        raise ValueError(f"Manifest validation failed: {msgs}")


def load_validated_manifest(path: str | Path, schema_name: str) -> Dict[str, Any]:
    """Load JSON file and validate it; returns the parsed object."""
    payload = _load_json(path)
    validate_json_manifest(payload, schema_name)
    return payload


class Settings(BaseSettings):
    """Environment-driven configuration for V5 service."""

    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")
    metrics_port: int = Field(9100, alias="METRICS_PORT")
    service_name: str = Field("dex-v5", alias="SERVICE_NAME")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    token_manifest_path: str = Field("config/v5/tokens.json", alias="TOKEN_MANIFEST_PATH")
    pool_manifest_path: str = Field("config/v5/pools.json", alias="POOL_MANIFEST_PATH")
    ws_config_path: str = Field("config/v5/ws_config.json", alias="WS_CONFIG_PATH")
    alarm_config_path: str = Field("config/v5/alarms.json", alias="ALARM_CONFIG_PATH")

    ethereum_rpc_url: str | None = Field(None, alias="ETHEREUM_RPC_URL")
    bsc_rpc_url: str | None = Field(None, alias="BSC_RPC_URL")
    polygon_rpc_url: str | None = Field(None, alias="POLYGON_RPC_URL")
    ethereum_ws_url: str | None = Field(None, alias="ETHEREUM_WS_URL")
    bsc_ws_url: str | None = Field(None, alias="BSC_WS_URL")
    # optional quoter overrides
    ethereum_quoter_addr: str | None = Field(None, alias="ETH_QUOTER_ADDR")
    bsc_quoter_addr: str | None = Field(None, alias="BSC_QUOTER_ADDR")
    # legacy aliases
    alchemy_ws_url: str | None = Field(None, alias="ALCHEMY_WS_URL")
    strategy_tri_threshold_bps: float = Field(10.0, alias="STRATEGY_TRI_THRESHOLD_BPS")
    initial_amount: float = Field(1.0, alias="STRATEGY_INITIAL_AMOUNT")
    generate_manifests: bool = Field(False, alias="V5_GEN_MANIFEST")
    resync_interval_seconds: int = Field(600, alias="RESYNC_INTERVAL_SECONDS")
    enable_poll_fallback: bool = Field(False, alias="ENABLE_POLL_FALLBACK")
    ws_sub_batch_size: int = Field(30, alias="WS_SUB_BATCH_SIZE")
    top_pools_per_exchange: int = Field(50, alias="V5_TOP_POOLS")
    rpc_max_qps: int = Field(8, alias="RPC_MAX_QPS")
    rpc_max_batch_size: int = Field(20, alias="RPC_MAX_BATCH_SIZE")

    # Load environment from standard dot-env files if present; ignore unrelated keys
    model_config = SettingsConfigDict(env_file=(".env", ".devstack-env"), case_sensitive=False, extra="ignore")

    # ------------------------------------------------------------------ #
    async def ensure_manifests(self, regen: bool | None = None) -> None:
        """Generate manifests asynchronously when allowed by flags."""
        regen_flag = regen if regen is not None else os.environ.get("V5_REGEN_MANIFEST") == "1"
        tokens_missing = not Path(self.token_manifest_path).exists()
        pools_missing = not Path(self.pool_manifest_path).exists()
        if not (regen_flag or self.generate_manifests):
            # If files missing and generation disabled, fail fast.
            if tokens_missing or pools_missing:
                raise FileNotFoundError(
                    "Manifests missing and auto-generation disabled. "
                    "Set V5_GEN_MANIFEST=1 or V5_REGEN_MANIFEST=1 to build manifests."
                )
            return
        if not (tokens_missing or pools_missing) and not regen_flag:
            return

        default_targets = f"eth:uniswap_v3:{self.top_pools_per_exchange},bsc:pancakeswap-v3-bsc:{self.top_pools_per_exchange}"
        targets = parse_targets(os.environ.get("V5_TARGETS", default_targets))
        api_key = os.environ.get("COINGECKO_API_KEY") or os.environ.get("COINGECKO_DEMO_API_KEY") or "CG-Lj3zoN2AMeZ9h9F94ew6XK7V"
        tier = os.environ.get("COINGECKO_TIER", "free")
        try:
            tokens, pools = await build_manifests_async(targets, api_key, tier)
        except Exception as exc:
            logging.getLogger(__name__).warning("Manifest generation failed (%s). Using existing manifests if present.", exc)
            # If files exist, keep going; else re-raise
            if not Path(self.token_manifest_path).exists() or not Path(self.pool_manifest_path).exists():
                raise
            return
        Path(self.token_manifest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.pool_manifest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.token_manifest_path).write_text(json.dumps(tokens, indent=2))
        Path(self.pool_manifest_path).write_text(json.dumps(pools, indent=2))
        logging.getLogger(__name__).info("Generated manifests via CoinGecko (targets=%s)", targets)

    # Convenience loaders -------------------------------------------------
    def load_tokens(self) -> Dict[str, Token]:
        import logging
        log = logging.getLogger(__name__)
        if not Path(self.token_manifest_path).exists() or os.environ.get("V5_REGEN_MANIFEST") == "1":
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    raise RuntimeError("Manifests missing; call await settings.ensure_manifests() before load_tokens() in async context")
            except RuntimeError:
                asyncio.run(self.ensure_manifests(self.generate_manifests))
        data = load_validated_manifest(self.token_manifest_path, "token_manifest.schema.json")
        tokens: Dict[str, Token] = {}
        log.info("Loading tokens from %s", self.token_manifest_path)
        for symbol, meta in data.get("tokens", {}).items():
            meta_copy = dict(meta)
            sym = meta_copy.pop("symbol", symbol)
            token = Token(symbol=sym, **meta_copy)
            tokens[token.symbol] = token
        log.info("Loaded %d tokens: %s", len(tokens), sorted(list(tokens.keys())[:20]))
        return tokens

    def load_pools(self) -> List[PoolMeta]:
        import logging
        log = logging.getLogger(__name__)
        if not Path(self.pool_manifest_path).exists() or os.environ.get("V5_REGEN_MANIFEST") == "1":
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    raise RuntimeError("Manifests missing; call await settings.ensure_manifests() before load_pools() in async context")
            except RuntimeError:
                asyncio.run(self.ensure_manifests(self.generate_manifests))
        data = load_validated_manifest(self.pool_manifest_path, "pool_manifest.schema.json")
        # Build decimal map from token manifest so price normalization is correct
        tokens_data = load_validated_manifest(self.token_manifest_path, "token_manifest.schema.json")
        decimals_map: Dict[str, int] = {}
        decimals_map_by_chain: Dict[tuple[str, str], int] = {}
        for sym_key, meta in tokens_data.get("tokens", {}).items():
            dec = meta.get("decimals", 18)
            chain = (meta.get("chain") or "").lower()
            sym = str(meta.get("symbol") or sym_key).upper()
            decimals_map.setdefault(sym, dec)
            if chain:
                decimals_map_by_chain[(sym, chain)] = dec

        pools: List[PoolMeta] = []
        log.info("Loading pools from %s", self.pool_manifest_path)
        # Build address lookup to canonicalize token order by address (matches Uniswap token0/1 ordering)
        addr_map: Dict[str, str] = {sym.upper(): meta.get("address", "").lower() for sym, meta in tokens_data.get("tokens", {}).items()}
        addr_map_by_chain: Dict[tuple[str, str], str] = {}
        for sym_key, meta in tokens_data.get("tokens", {}).items():
            chain = (meta.get("chain") or "").lower()
            sym = str(meta.get("symbol") or sym_key).upper()
            addr = meta.get("address", "").lower()
            if chain:
                addr_map_by_chain[(sym_key.upper(), chain)] = addr
                addr_map_by_chain[(sym, chain)] = addr

        for entry in data.get("pairs", []):
            token0 = entry["token0"]
            token1 = entry["token1"]
            # Prefer addresses matching the pool's network for correct token0/token1 ordering
            for _, pool_cfg in (entry.get("pools") or {}).items():
                meta = pool_cfg.get("metadata") or {}
                network = (meta.get("network") or "").lower()
                addr0 = addr_map_by_chain.get((str(token0).upper(), network)) or addr_map.get(str(token0).upper())
                addr1 = addr_map_by_chain.get((str(token1).upper(), network)) or addr_map.get(str(token1).upper())

                # Canonicalize order: token0 should have lower address than token1
                tok0, tok1 = token0, token1
                if addr0 and addr1 and addr1 < addr0:
                    tok0, tok1 = token1, token0
                    addr0, addr1 = addr1, addr0

                dec0 = decimals_map_by_chain.get((str(tok0).upper(), network), decimals_map.get(str(tok0).upper(), 18))
                dec1 = decimals_map_by_chain.get((str(tok1).upper(), network), decimals_map.get(str(tok1).upper(), 18))
                pools.append(
                    PoolMeta(
                        pool_address=pool_cfg["address"],
                        platform=meta["platform"],
                        token0=tok0,
                        token1=tok1,
                        fee_tier=meta["fee"],
                        network=meta["network"],
                        dec0=dec0,
                        dec1=dec1,
                    )
                )
        log.info("Loaded %d pools", len(pools))
        if pools:
            for i, pool in enumerate(pools[:5]):
                log.info(
                    "  Pool[%d]: %s/%s on %s (fee=%d, addr=%s, dec0=%d, dec1=%d)",
                    i,
                    pool.token0,
                    pool.token1,
                    pool.network,
                    pool.fee_tier,
                    pool.pool_address[:8],
                    pool.dec0,
                    pool.dec1,
                )
        return pools

    def load_alarm_config(self) -> AlarmConfig:
        data = load_validated_manifest(self.alarm_config_path, "alarms.schema.json")
        return AlarmConfig(
            stale_snapshot_ms=data["thresholds"]["stale_snapshot_ms"],
            stale_ws_ms=data["thresholds"]["stale_ws_ms"],
            missing_data_threshold=data["thresholds"].get("missing_data", 0),
            webhook_urls=data.get("webhooks", []),
        )

    @property
    def manifest_tokens_path(self) -> str:
        return self.token_manifest_path

    @property
    def manifest_pools_path(self) -> str:
        return self.pool_manifest_path


__all__ = [
    "Settings",
    "validate_json_manifest",
    "load_validated_manifest",
]
