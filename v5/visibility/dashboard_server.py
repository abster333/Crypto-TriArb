"""Minimal FastAPI dashboard server."""

from __future__ import annotations

from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, HTMLResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from v5.state_store.state_reader import StateReader
from v5.simulator.realtime_detector import RealtimeArbDetector
from v5.common.models import Opportunity


_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>DEX Pools Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0b1021; color: #e7ecf3; }
    h2 { margin-bottom: 6px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #222c45; text-align: left; }
    th { background: #14213d; position: sticky; top: 0; }
    tr:nth-child(even) { background: #11182f; }
    .stale { color: #ffb347; }
    .meta { color: #8aa0c8; font-size: 12px; margin-bottom: 10px; }
  </style>
</head>
<body>
  <h2>DEX Pools</h2>
  <div class="meta">Live WS L1 summary (auto-refresh 2s)</div>
  <div id="updated">Loading...</div>
    <table id="grid">
      <thead>
        <tr>
        <th>Pair</th><th>Network</th><th>Fee (bps)</th><th>Price</th><th>Depth ±0.5%</th><th>Last WS (ms)</th><th>Address</th>
        </tr>
      </thead>
      <tbody></tbody>
  </table>
  <script>
    async function refresh() {
      try {
        const res = await fetch('/pools/summary');
        const data = await res.json();
        const rows = data.pools || [];
        const tbody = document.querySelector('#grid tbody');
        tbody.innerHTML = '';
        rows.forEach(r => {
          const tr = document.createElement('tr');
          const age = r.last_ws_ms || 0;
          if (age > 30_000) tr.classList.add('stale');
          tr.innerHTML = `
            <td>${r.pair}</td>
            <td>${r.network}</td>
            <td>${r.fee_bps ?? ''}</td>
            <td>${r.price ?? ''}</td>
            <td>${r.liquidity ?? ''}</td>
            <td>${age}</td>
            <td>${r.address}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""

# Live cycles / opportunities page (lightweight, auto-refresh)
_HTML_CYCLES = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>DEX Cycles</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0b1021; color: #e7ecf3; }
    h2 { margin-bottom: 6px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #222c45; text-align: left; }
    th { background: #14213d; position: sticky; top: 0; }
    tr:nth-child(even) { background: #11182f; }
    .stale { color: #ffb347; }
    .meta { color: #8aa0c8; font-size: 12px; margin-bottom: 10px; }
    .pill { padding: 2px 6px; border-radius: 6px; font-size: 12px; background: #1d2b4f; color: #9cd3ff; }
    .neg { color: #ff8a8a; }
    .pos { color: #7cf29c; }
    .leg { display: block; font-size: 12px; color: #b9c6e4; }
  </style>
</head>
<body>
  <h2>Live Cycles</h2>
  <div class="meta">All cycles (auto-refresh 2s, includes negative/invalid)</div>
  <div id="updated">Loading...</div>
  <table id="grid">
    <thead>
      <tr>
        <th>Cycle</th><th>Status</th><th>ROI (bps)</th><th>Profit (start token)</th><th>Size Cap (start token)</th><th>Age (s)</th><th>Legs</th><th>Quoter</th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
  <script>
    async function refresh() {
      try {
        const res = await fetch('/api/cycles/live?limit=500');
        const rows = await res.json();
        const tbody = document.querySelector('#grid tbody');
        tbody.innerHTML = '';
        rows.forEach(r => {
          const tr = document.createElement('tr');
          const roi = (r.roi_bps ?? 0).toFixed(2);
          const cls = roi >= 0 ? 'pos' : 'neg';
          const legsHtml = (r.legs || []).map(l => `<span class="leg">${l.token_in}->${l.token_out} @ ${String(l.pool||'').slice(0,8)} | impact=${(l.price_impact?.toFixed ? l.price_impact.toFixed(6) : (l.price_impact ?? ''))}</span>`).join('');
          const quoter = r.quoter
            ? (r.quoter.error
                ? `error=${r.quoter.error}`
                : `in=${r.quoter.amount_in || ''}, out=${r.quoter.amount_out || ''}, profit=${r.quoter.profit || ''}, roi_bps=${r.quoter.roi_bps || ''}`)
            : '';
          tr.innerHTML = `
            <td>${r.cycle}</td>
            <td><span class="pill">${r.status || ''}${r.reason ? ' ('+r.reason+')' : ''}</span></td>
            <td class="${cls}">${roi}</td>
            <td>${r.profit ?? ''}</td>
            <td>${r.size_cap ?? ''}</td>
            <td>${r.max_age_ms != null ? (r.max_age_ms/1000).toFixed(1) : ''}</td>
            <td>${legsHtml}</td>
            <td>${quoter}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""


class DashboardServer:
    def __init__(self, state_reader: StateReader, detector: RealtimeArbDetector):
        self.state_reader = state_reader
        self.detector = detector
        self.app = FastAPI()
        self._wire_routes()

    def _wire_routes(self) -> None:
        @self.app.get("/health")
        async def health():
            addrs = await self.state_reader.list_all_pool_addresses()
            pools = await self.state_reader.get_all_pool_states(addrs) if addrs else {}
            return {"pools": len(pools)}

        @self.app.get("/pools")
        async def pools():
            addrs = await self.state_reader.list_all_pool_addresses()
            pools = await self.state_reader.get_all_pool_states(addrs) if addrs else {}
            return pools

        @self.app.get("/pools/summary")
        async def pools_summary():
            addrs = await self.state_reader.list_all_pool_addresses()
            pools = await self.state_reader.get_all_pool_states(addrs) if addrs else {}
            out = []
            for addr, payload in pools.items():
                meta = payload.get("pool_meta") or {}
                state = payload.get("pool_state") or payload
                token0 = meta.get("token0_symbol") or meta.get("token0") or "?"
                token1 = meta.get("token1_symbol") or meta.get("token1") or "?"
                pair = f"{token0}/{token1}"
                fee_tier = meta.get("fee_tier") or meta.get("fee") or meta.get("fee_bps")
                fee_bps = fee_tier / 100 if fee_tier else None  # Uniswap v3 fee_tier ppm -> bps
                network = (meta.get("network") or "").upper() or meta.get("chain") or "UNKNOWN"
                price = state.get("price")
                dec0 = meta.get("dec0") or 18
                dec1 = meta.get("dec1") or 18
                liquidity_raw = state.get("liquidity_int") or state.get("liquidity")
                depth_str = ""
                def _fmt(v: float) -> str:
                    if v is None:
                        return ""
                    av = abs(v)
                    if av >= 1e12:
                        return f"{v/1e12:.2f}T"
                    if av >= 1e9:
                        return f"{v/1e9:.2f}B"
                    if av >= 1e6:
                        return f"{v/1e6:.2f}M"
                    if av >= 1e3:
                        return f"{v/1e3:.2f}K"
                    return f"{v:.2f}"

                try:
                    from decimal import Decimal, getcontext
                    getcontext().prec = 50
                    L = Decimal(str(liquidity_raw))
                    sqrt_price_x96 = Decimal(str(state.get("sqrt_price_x96") or "0"))
                    if L > 0 and sqrt_price_x96 > 0:
                        TWO96 = Decimal(2) ** 96
                        scale = Decimal(10) ** (dec0 - dec1)
                        sqrt_cur = sqrt_price_x96 / TWO96
                        price_cur = (sqrt_cur * sqrt_cur) * scale
                        band = Decimal("0.005")  # ±0.5%
                        # target prices
                        price_down = price_cur * (Decimal(1) - band)
                        price_up = price_cur * (Decimal(1) + band)
                        def sqrt_from_price(p):
                            if p <= 0:
                                return None
                            return (p / scale).sqrt()
                        sqrt_down = sqrt_from_price(price_down)
                        sqrt_up = sqrt_from_price(price_up)
                        amt0 = amt1 = None
                        if sqrt_down and sqrt_down > 0:
                            amt0 = L * (sqrt_cur - sqrt_down) / (sqrt_cur * sqrt_down)
                        if sqrt_up:
                            amt1 = L * (sqrt_up - sqrt_cur)
                        # Convert raw amounts to human tokens using decimals
                        denom0 = Decimal(10) ** dec0
                        denom1 = Decimal(10) ** dec1
                        amt0_tokens = amt0 / denom0 if amt0 is not None else None
                        amt1_tokens = amt1 / denom1 if amt1 is not None else None

                        def _to_float(v):
                            try:
                                return float(v)
                            except Exception:
                                return None
                        amt0_f = _to_float(amt0_tokens)
                        amt1_f = _to_float(amt1_tokens)
                        if amt0_f is not None and amt1_f is not None:
                            depth_str = f"{_fmt(amt0_f)} {token0} ↓ / {_fmt(amt1_f)} {token1} ↑"
                        elif amt0_f is not None:
                            depth_str = f"{_fmt(amt0_f)} {token0} ↓"
                        elif amt1_f is not None:
                            depth_str = f"{_fmt(amt1_f)} {token1} ↑"
                        else:
                            depth_str = "N/A"
                except Exception:
                    depth_str = ""
                out.append(
                    {
                        "address": addr,
                        "pair": pair,
                        "fee_bps": fee_bps,
                        "network": network,
                        "price": price,
                        "liquidity": depth_str,
                        "last_ws_ms": payload.get("last_ws_refresh_ms"),
                    }
                )
            return {"pools": out}

        @self.app.get("/", response_class=HTMLResponse)
        async def root():
            return HTMLResponse(_HTML)

        @self.app.get("/pools/{address}")
        async def pool(address: str):
            data = await self.state_reader.get_pool_state(address)
            if not data:
                raise HTTPException(status_code=404, detail="pool not found")
            return data

        @self.app.get("/opportunities")
        async def opportunities():
            return [opp.model_dump() for opp in self.detector.recent_opportunities()]

        @self.app.get("/opportunities/recent")
        async def recent_opportunities(limit: int = 50):
            return [opp.model_dump() for opp in self.detector.get_recent_opportunities(limit)]

        @self.app.get("/cycles")
        async def cycles_page():
            return HTMLResponse(_HTML_CYCLES)

        @self.app.get("/api/cycles/live")
        async def api_cycles_live(limit: int = 500):
            """
            Returns ALL cycles with their latest simulated ROI, even if negative or missing data.
            """
            data = await self.detector.scenario_runner.snapshot_cycles()
            if limit:
                data = data[:limit]
            return data

        @self.app.get("/data-quality")
        async def data_quality():
            addrs = await self.state_reader.list_all_pool_addresses()
            pools = await self.state_reader.get_all_pool_states(addrs) if addrs else {}
            total = len(pools)
            stale = sum(1 for p in pools.values() if p.get("stream_ready") is False)
            ws_connected = sum(1 for p in pools.values() if p.get("last_ws_refresh_ms"))
            quality_score = (total - stale) / total if total else 0
            ws_coverage = ws_connected / total if total else 0
            return {
                "total_pools": total,
                "stale_pools": stale,
                "ws_connected": ws_connected,
                "quality_score": quality_score,
                "ws_coverage": ws_coverage,
            }

        @self.app.get("/metrics")
        async def metrics():
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

        @self.app.get("/ws/status")
        async def ws_status():
            addrs = await self.state_reader.list_all_pool_addresses()
            pools = await self.state_reader.get_all_pool_states(addrs) if addrs else {}
            networks = {}
            for payload in pools.values():
                meta = payload.get("pool_meta") or {}
                network = (meta.get("network") or "").lower() or "unknown"
                last_ms = payload.get("last_ws_refresh_ms") or 0
                stats = networks.setdefault(network, {"pools": 0, "last_message_ms": 0})
                stats["pools"] += 1
                stats["last_message_ms"] = max(stats["last_message_ms"], last_ms)
            return {"networks": [{"network": k, **v, "connected": bool(v["last_message_ms"])} for k, v in networks.items()]}

    async def start(self):  # For symmetry; FastAPI is mounted externally
        return self.app


__all__ = ["DashboardServer"]
