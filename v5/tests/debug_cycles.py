"""Standalone script to debug cycle detection."""

from __future__ import annotations

from v5.common.config import Settings
from v5.simulator.graph import TokenGraph


def main() -> None:
    s = Settings()
    tokens = s.load_tokens()
    pools = s.load_pools()
    print("=" * 60)
    print("CYCLE DETECTION TEST")
    print("=" * 60)
    print(f"Tokens: {len(tokens)}")
    print(f"Pools: {len(pools)}")

    graph = TokenGraph(pools)
    token_symbols = list(tokens.keys())

    # Legacy (deduped) cycles
    cycles = graph.find_triangular_cycles(token_symbols)
    print(f"\nDeduped triangle count (one direction per unordered triple): {len(cycles)}")
    if cycles:
        print("Sample deduped cycles:")
        for i, cycle in enumerate(cycles[:5]):
            print(f"  [{i}] {' -> '.join(cycle)} -> {cycle[0]}")

    # Expanded: all directed permutations with all fee-tier combos
    variants = graph.find_triangular_cycle_variants(token_symbols)
    print(f"\nExpanded cycle variants (directed × fee tiers): {len(variants)}")
    if variants:
        print("Sample variants:")
        for i, var in enumerate(variants[:5]):
            hops = [f"{e.base}->{e.quote}@{int(e.fee*1_000_000)}bps" for e in var.edges]
            pools_short = [e.pool.pool_address[:8] for e in var.edges]
            print(f"  [{i}] {' -> '.join(var.tokens)} -> {var.tokens[0]} | hops={hops} pools={pools_short}")

    if not cycles and not variants:
        print("\n❌ FAILED: No cycles found!")
        print(f"  - Edges: {len(graph.edges)}")
        unique_tokens = {p.token0 for p in pools} | {p.token1 for p in pools}
        print(f"  - Unique tokens in pools: {len(unique_tokens)}")
        print(f"  - Token symbols provided: {len(token_symbols)}")


if __name__ == "__main__":
    main()
