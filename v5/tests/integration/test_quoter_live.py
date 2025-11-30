import os
from decimal import Decimal

import pytest
pytest.importorskip("pytest_asyncio")

from v5.simulator.quoter_client import quote_exact_input_single, quote_exact_input


PUBLIC_BSC_RPCS = [
    "https://bsc-dataseed.binance.org/",
    "https://bsc-dataseed1.ninicoin.io/",
    "https://bsc-dataseed1.defibit.io/",
    "https://rpc.ankr.com/bsc",
]

QUOTER_CANDIDATES = [
    "0xb8195f93ddedc0d8b45d4ecf16e404210c835ecc",
    "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997",
]

WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
USDT = "0x55d398326f99059ff775485246999027b3197955"
DEC_WBNB = 18
FEES = [100, 500, 2500, 10000]


def pick_rpcs():
    rpcs = []
    env = os.environ.get("BSC_RPC_URL")
    if env:
        rpcs.append(env)
    rpcs.extend(PUBLIC_BSC_RPCS)
    return rpcs


@pytest.mark.integration
@pytest.mark.asyncio
async def test_quoter_live_wbnb_usdt():
    rpcs = pick_rpcs()
    amount_in = Decimal("0.05")
    amount_raw = int(amount_in * (Decimal(10) ** DEC_WBNB))
    errors = []
    for rpc in rpcs:
        for quoter in QUOTER_CANDIDATES:
            for fee in FEES:
                res = await quote_exact_input_single(rpc, WBNB, USDT, fee, amount_raw, quoter=quoter)
                if res is None:
                    # Try path-based quote as fallback
                    res = await quote_exact_input(rpc, [WBNB, USDT], [fee], amount_raw, quoter=quoter)
                if res is None:
                    errors.append((rpc, quoter, fee))
                    continue
                assert res.amount_out > 0
                implied_price = Decimal(res.amount_out) / Decimal(amount_raw)
                # sanity: WBNB/USDT price should be roughly between 100 and 1,000 at current market; wide but not absurd
                assert implied_price > Decimal("100")
                assert implied_price < Decimal("1000")
                print(f"SUCCESS rpc={rpc} quoter={quoter} fee={fee} amount_out={res.amount_out} implied_price={implied_price}")
                return
    pytest.fail(f"All quoter calls reverted. Attempts: {errors[:5]} ... total {len(errors)}")
