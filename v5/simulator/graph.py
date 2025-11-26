"""Deterministic token graph builder."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Dict, List, Tuple

from v5.common.models import PoolMeta


@dataclass(slots=True)
class Edge:
    base: str
    quote: str
    pool: PoolMeta
    fee: float


@dataclass(slots=True)
class CycleVariant:
    """Represents a concrete directed 3-token cycle with specific pools per hop."""

    tokens: List[str]
    edges: List[Edge]


class TokenGraph:
    def __init__(self, pools: List[PoolMeta]) -> None:
        self.pools = pools
        # Map directed token pair -> list of candidate edges (one per fee-tier pool)
        self.edges: Dict[Tuple[str, str], List[Edge]] = {}
        self._build()

        # Diagnostic logging
        import logging
        log = logging.getLogger(__name__)
        log.info("=" * 60)
        log.info("GRAPH CONSTRUCTION DIAGNOSTICS")
        log.info("=" * 60)
        log.info("Input pools: %d", len(pools))
        for i, pool in enumerate(pools[:5]):
            log.info("Pool[%d]: %s/%s on %s (addr=%s, fee=%d)", i, pool.token0, pool.token1, pool.network, pool.pool_address[:8], pool.fee_tier)
        if len(pools) > 5:
            log.info("... and %d more pools", len(pools) - 5)
        log.info("Edges built: %d", len(self.edges))
        for i, ((t0, t1), edges) in enumerate(list(self.edges.items())[:10]):
            first = edges[0]
            log.info(
                "Edge[%d]: %s -> %s (candidates=%d, sample_pool=%s)",
                i,
                t0,
                t1,
                len(edges),
                first.pool.pool_address[:8],
            )
        if len(self.edges) > 10:
            log.info("... and %d more edges", len(self.edges) - 10)
        unique_tokens = set()
        for pool in pools:
            unique_tokens.add(pool.token0)
            unique_tokens.add(pool.token1)
        log.info("Unique tokens: %d - %s", len(unique_tokens), sorted(list(unique_tokens)[:20]))
        log.info("=" * 60)

    def _build(self) -> None:
        for p in self.pools:
            fee = (p.fee_tier or 0) / 1_000_000 if p.fee_tier else 0.0
            edge_ab = Edge(p.token0, p.token1, p, fee)
            edge_ba = Edge(p.token1, p.token0, p, fee)
            self.edges.setdefault((p.token0, p.token1), []).append(edge_ab)
            self.edges.setdefault((p.token1, p.token0), []).append(edge_ba)

    def path(self, cycle: List[str]) -> List[Edge]:
        """Return a single deterministic edge per hop (first candidate)."""

        edges: List[Edge] = []
        for a, b in zip(cycle, cycle[1:] + cycle[:1]):
            candidates = self.edges.get((a, b)) or []
            if candidates:
                edges.append(candidates[0])
        return edges

    def path_variants(self, cycle: List[str]) -> List[List[Edge]]:
        """Return all candidate edge combinations for a directed 3-token cycle.

        Each hop may have multiple fee-tier pools; we enumerate the cartesian
        product of candidates so callers can evaluate every concrete route.
        Returns an empty list if any hop is missing.
        """

        hops: List[List[Edge]] = []
        for a, b in zip(cycle, cycle[1:] + cycle[:1]):
            candidates = self.edges.get((a, b)) or []
            if not candidates:
                return []
            hops.append(candidates)
        variants: List[List[Edge]] = []
        for combo in product(*hops):
            variants.append(list(combo))
        return variants

    def find_triangular_cycles(self, base_tokens: List[str]) -> List[List[str]]:
        import logging
        log = logging.getLogger(__name__)

        log.info("=" * 60)
        log.info("CYCLE DETECTION DIAGNOSTICS")
        log.info("=" * 60)
        log.info("Base tokens provided: %d - %s", len(base_tokens), base_tokens[:10])

        tokens = list({p.token0 for p in self.pools} | {p.token1 for p in self.pools})
        log.info("Tokens from pools: %d - %s", len(tokens), sorted(tokens[:20]))

        base_set = set(base_tokens)
        pool_set = set(tokens)
        overlap = base_set & pool_set
        log.info("Overlap between base_tokens and pool tokens: %d", len(overlap))
        if len(overlap) < len(base_set):
            missing = base_set - pool_set
            if missing:
                log.warning("Base tokens NOT in pools: %s", missing)

        cycles: List[List[str]] = []
        potential_cycles = 0
        for a in tokens:
            for b in tokens:
                if b == a:
                    continue
                for c in tokens:
                    if c in (a, b):
                        continue
                    potential_cycles += 1
                    has_ab = bool(self.edges.get((a, b)))
                    has_bc = bool(self.edges.get((b, c)))
                    has_ca = bool(self.edges.get((c, a)))
                    if has_ab and has_bc and has_ca:
                        cycle = [a, b, c]
                        cycles.append(cycle)
                        if len(cycles) <= 5:
                            log.info("Found cycle #%d: %s -> %s -> %s", len(cycles), a, b, c)

        log.info("Checked %d potential 3-token combinations", potential_cycles)
        log.info("Found %d raw cycles (before dedup)", len(cycles))

        unique = []
        seen = set()
        for cyc in cycles:
            key = tuple(sorted(cyc))
            if key not in seen:
                seen.add(key)
                unique.append(cyc)

        log.info("After dedup: %d unique cycles", len(unique))
        if unique:
            for i, cyc in enumerate(unique[:10]):
                log.info("  Cycle[%d]: %s", i, " -> ".join(cyc))
        else:
            log.error("NO CYCLES FOUND - INVESTIGATING WHY...")
            log.error("Edges available: %d", len(self.edges))
            log.error("Tokens available: %d", len(tokens))
            if len(tokens) >= 3 and len(self.edges) >= 3:
                sample_tokens = list(tokens)[:min(5, len(tokens))]
                log.error("Sample edge check for tokens: %s", sample_tokens)
                for t1 in sample_tokens:
                    for t2 in sample_tokens:
                        if t1 == t2:
                            continue
                        edge = self.edges.get((t1, t2))
                        if edge:
                            log.error("  Edge %s -> %s EXISTS (pool %s)", t1, t2, edge.pool.pool_address[:8])
                        else:
                            log.error("  Edge %s -> %s MISSING", t1, t2)
            else:
                log.error("Not enough edges or tokens to form cycles")

        log.info("=" * 60)
        return unique

    def find_triangular_cycle_variants(self, base_tokens: List[str]) -> List[CycleVariant]:
        """Enumerate all directed cycles (6 per triangle) and expand fee-tier combos.

        This does *not* dedupe permutations; every ordered cycle with concrete pool
        selections is returned so callers can evaluate each distinct route.
        """

        import logging
        log = logging.getLogger(__name__)

        tokens = list({p.token0 for p in self.pools} | {p.token1 for p in self.pools})
        variants: List[CycleVariant] = []

        for a in tokens:
            for b in tokens:
                if b == a:
                    continue
                for c in tokens:
                    if c in (a, b):
                        continue
                    # generate both directions for this ordered triple by running loop over tokens permutations
                    cycle = [a, b, c]
                    combos = self.path_variants(cycle)
                    for edges in combos:
                        # Enforce single-network cycles: all pools must be on the same network
                        networks = {e.pool.network.lower() for e in edges}
                        if len(networks) != 1:
                            continue
                        variants.append(CycleVariant(tokens=cycle, edges=edges))

        log.info("Cycle variants (directed with fee tiers): %d", len(variants))
        return variants
