from v5.simulator.graph import TokenGraph
from v5.common.models import PoolMeta


def test_graph_builds_edges():
    meta = PoolMeta(pool_address="0x" + "2"*40, platform="UNIV3", token0="A", token1="B", fee_tier=500, network="eth")
    g = TokenGraph([meta])
    assert ("A", "B") in g.edges
    assert ("B", "A") in g.edges
