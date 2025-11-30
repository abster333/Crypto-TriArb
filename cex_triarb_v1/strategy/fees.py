from __future__ import annotations

import os
from typing import Dict

# Conservative public defaults (taker bps). These can be overridden via env.
DEFAULT_TAKER_BPS = {
    "COINBASE": 6.0,   # 0.06%
    "KRAKEN": 16.0,    # 0.16%
    "OKX": 10.0,       # 0.10%
}


def fee_map(exchanges) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for ex in exchanges:
        ex_u = ex.upper()
        env_key = f"FEE_{ex_u}_BPS"
        fee = os.getenv(env_key)
        if fee is not None:
            try:
                out[ex_u] = float(fee)
                continue
            except ValueError:
                pass
        out[ex_u] = DEFAULT_TAKER_BPS.get(ex_u, 10.0)
    return out
