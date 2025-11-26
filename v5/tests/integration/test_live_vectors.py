import json
from pathlib import Path
from decimal import Decimal

import pytest

from v5.simulator import uniswap_v3_math as mathv3


@pytest.mark.parametrize("entry", json.loads(Path(__file__).resolve().parents[1].joinpath("fixtures/slot0_live.json").read_text()))
def test_live_slot0_vectors(entry):
    sqrt = Decimal(entry["sqrt_price_x96"])
    price = mathv3.price_from_sqrt_price(sqrt, entry["decimals_token0"], entry["decimals_token1"])
    # sanity: price should be positive and finite
    assert price > 0
    tick = mathv3.tick_from_sqrt_price(sqrt)
    assert isinstance(tick, int)
