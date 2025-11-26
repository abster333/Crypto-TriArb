from __future__ import annotations

from typing import Dict, List, Set, Tuple

from cex_triarb_v1.strategy.consumer import Cycle, Leg, parse_symbol


def generate_cycles_for_exchange(exchange: str, symbols: Set[str]) -> List[Cycle]:
    """
    Build simple triangular cycles of the form:
      A/B (BUY), A/C (SELL), C/B (SELL)
    using available symbols for a single exchange.
    Also emit the reverse direction:
      C/B (BUY), A/C (BUY), A/B (SELL)
    """
    parsed: Dict[str, Tuple[str, str]] = {}
    for s in symbols:
        parsed_sym = parse_symbol(s)
        if parsed_sym:
            parsed[s] = parsed_sym
    pair_exists = {(base, quote): sym for sym, (base, quote) in parsed.items()}
    currencies = set()
    for base, quote in pair_exists:
        currencies.add(base)
        currencies.add(quote)
    cycles: Dict[str, Cycle] = {}
    currency_list = list(currencies)
    for a in currency_list:
        for b in currency_list:
            if b == a:
                continue
            for c in currency_list:
                if c == a or c == b:
                    continue
                if (a, b) in pair_exists and (a, c) in pair_exists and (c, b) in pair_exists:
                    leg1 = Leg(exchange=exchange, symbol=pair_exists[(a, b)], side="BUY")
                    leg2 = Leg(exchange=exchange, symbol=pair_exists[(a, c)], side="SELL")
                    leg3 = Leg(exchange=exchange, symbol=pair_exists[(c, b)], side="SELL")
                    cid = "|".join([f"{leg1.symbol}", f"{leg2.symbol}", f"{leg3.symbol}"])
                    if cid not in cycles:
                        cycles[cid] = Cycle(legs=[leg1, leg2, leg3], id=f"{exchange}:{cid}")
                    # Reverse direction: start in B, buy C, buy A, sell back to B.
                    r_leg1 = Leg(exchange=exchange, symbol=pair_exists[(c, b)], side="BUY")
                    r_leg2 = Leg(exchange=exchange, symbol=pair_exists[(a, c)], side="BUY")
                    r_leg3 = Leg(exchange=exchange, symbol=pair_exists[(a, b)], side="SELL")
                    r_cid = "|".join([f"{r_leg1.symbol}", f"{r_leg2.symbol}", f"{r_leg3.symbol}"])
                    if r_cid not in cycles:
                        cycles[r_cid] = Cycle(legs=[r_leg1, r_leg2, r_leg3], id=f"{exchange}:{r_cid}")
                    # Third orientation: start in C, buy A with C, sell A for B, buy C with B (cycle returns to C).
                    t_leg1 = Leg(exchange=exchange, symbol=pair_exists[(a, c)], side="BUY")
                    t_leg2 = Leg(exchange=exchange, symbol=pair_exists[(a, b)], side="SELL")
                    t_leg3 = Leg(exchange=exchange, symbol=pair_exists[(c, b)], side="BUY")
                    t_cid = "|".join([f"{t_leg1.symbol}", f"{t_leg2.symbol}", f"{t_leg3.symbol}"])
                    if t_cid not in cycles:
                        cycles[t_cid] = Cycle(legs=[t_leg1, t_leg2, t_leg3], id=f"{exchange}:{t_cid}")
    return list(cycles.values())
