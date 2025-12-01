from __future__ import annotations

import json
import time
import logging
from typing import Dict, List, Optional

import fastapi
from fastapi.responses import HTMLResponse, JSONResponse
from cex_triarb_v1.ingest.publishers import connect_nats_with_retry

try:
    import nats
except ImportError:  # pragma: no cover
    nats = None
try:
    import redis.asyncio as redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None

log = logging.getLogger(__name__)

# Keep quote parsing aligned with strategy math.
QUOTE_SUFFIXES = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP"]


def parse_symbol(sym: str) -> tuple[str | None, str | None]:
    s = sym.upper().replace("/", "").replace("-", "").replace("_", "")
    for suf in QUOTE_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            return s[: -len(suf)], suf
    return None, None


class DepthCache:
    def __init__(self) -> None:
        self.data: Dict[tuple[str, str], dict] = {}
        self.latency_stats: Dict[str, dict] = {}

    def update(self, event: dict) -> None:
        exchange = event.get("exchange")
        symbol = event.get("symbol")
        if not exchange or not symbol:
            return
        ts_event = event.get("ts_event")
        ts_ingest = event.get("ts_ingest")
        latency_ms = None
        if ts_event and ts_ingest and ts_ingest >= ts_event:
            latency_ms = ts_ingest - ts_event
        self.data[(exchange, symbol)] = {
            "exchange": exchange,
            "symbol": symbol,
            "bids": event.get("bids", []),
            "asks": event.get("asks", []),
            "depth": event.get("depth"),
            "ts_event": event.get("ts_event"),
            "ts_ingest": event.get("ts_ingest"),
            "source": event.get("source"),
            "latency_ms": latency_ms,
        }
        if latency_ms is not None:
            stats = self.latency_stats.setdefault(exchange, {"sum": 0.0, "count": 0})
            stats["sum"] += latency_ms
            stats["count"] += 1

    def snapshot(self) -> List[dict]:
        out: List[dict] = []
        now_ms = int(time.time() * 1000)
        for row in self.data.values():
            out.append(
                {
                    **row,
                    "age_ms": max(0, now_ms - (row.get("ts_ingest") or 0)),
                }
            )
        return sorted(out, key=lambda r: (r["symbol"], r["exchange"]))

    def latency_summary(self) -> List[dict]:
        out: List[dict] = []
        for ex, stats in self.latency_stats.items():
            if stats["count"] == 0:
                continue
            out.append({"exchange": ex, "avg_latency_ms": stats["sum"] / stats["count"], "samples": stats["count"]})
        return out


class DepthDashboardService:
    def __init__(
        self,
        nats_url: str = "nats://127.0.0.1:4222",
        snapshot_subject: str = "md.depth",
        opp_subject: str | None = "strategy.depth.opportunity",
        symbols: Optional[list[str]] = None,
        cycles_subject: str | None = "strategy.depth.cycles",
        redis_url: str = "redis://127.0.0.1:6379/0",
        redis_prefix: str = "md_depth",
    ) -> None:
        self.nats_url = nats_url
        self.snapshot_subject = snapshot_subject
        self.opp_subject = opp_subject
        self.cycles_subject = cycles_subject
        self.symbols = [s.upper() for s in symbols] if symbols else None
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix.rstrip(":")
        self.cache = DepthCache()
        self._nc = None
        self._sub = None
        self._opp_sub = None
        self._cycles_sub = None
        self._redis = None
        self.opps: List[dict] = []
        self.cycles: List[dict] = []
        self.last_opp_by_cycle: Dict[str, dict] = {}
        self.app = fastapi.FastAPI(title="CEX Depth Dashboard")
        self._mount_routes()

    def _mount_routes(self) -> None:
        @self.app.get("/healthz")
        async def healthz():
            return {"status": "ok", "count": len(self.cache.data)}

        @self.app.get("/api/depth")
        async def api_depth():
            return JSONResponse(self.cache.snapshot())

        @self.app.get("/api/opps")
        async def api_opps():
            return JSONResponse(self.opps)

        @self.app.get("/api/latency")
        async def api_latency():
            return JSONResponse(self.cache.latency_summary())

        @self.app.get("/", response_class=HTMLResponse)
        async def index():
            return HTMLResponse(_HTML_PAGE)

        @self.app.get("/opps", response_class=HTMLResponse)
        async def opps_page():
            return HTMLResponse(_HTML_OPPS_PAGE)

        @self.app.get("/api/cycles")
        async def api_cycles():
            cycles = self.cycles or []
            data = []
            for c in cycles:
                leg_info = []
                missing = False
                amt = 1.0  # start with 1 unit of quote on leg1
                start_ccy = ""
                end_ccy = ""
                cid = c.get("id")
                opp = self.last_opp_by_cycle.get(cid) if cid else None
                for leg in c.get("legs", []):
                    ex = leg.get("exchange", "").upper()
                    sym = leg.get("symbol", "").upper()
                    side = leg.get("side", "").upper()
                    book = self.cache.data.get((ex, sym))
                    bids = book.get("bids") if book else None
                    asks = book.get("asks") if book else None
                    bid = bids[0][0] if bids else None
                    ask = asks[0][0] if asks else None
                    bid_sz = bids[0][1] if bids and len(bids[0]) > 1 else None
                    ask_sz = asks[0][1] if asks and len(asks[0]) > 1 else None
                    price = ask if side == "BUY" else bid
                    size_avail = ask_sz if side == "BUY" else bid_sz
                    if price is None or price <= 0 or (size_avail is not None and size_avail <= 0):
                        missing = True
                        leg_info.append({"ex": ex, "sym": sym, "side": side, "price": price, "size": size_avail})
                        break
                    base, quote = parse_symbol(sym)
                    if side == "BUY":
                        take = min(size_avail or amt / price, amt / price)
                        amt = take
                        if not start_ccy:
                            start_ccy = quote or ""
                        end_ccy = base or ""
                    else:
                        take = min(size_avail or amt, amt)
                        amt = take * price
                        if not start_ccy:
                            start_ccy = base or ""
                        end_ccy = quote or ""
                    leg_info.append({"ex": ex, "sym": sym, "side": side, "price": price, "size": size_avail})
                roi_bps = None
                gross_roi_bps = None
                ts_detected = None
                age_ms = None
                if opp:
                    roi_bps = opp.get("roi_bps")
                    gross_roi_bps = opp.get("gross_roi_bps")
                    ts_detected = opp.get("ts_detected")
                    if ts_detected:
                        age_ms = max(0, int(time.time() * 1000) - int(ts_detected))
                    leg_info = opp.get("leg_fills_net") or leg_info
                elif not missing and leg_info:
                    start_amt = 1.0
                    end_amt = amt
                    roi_bps = (end_amt - start_amt) / start_amt * 10_000
                data.append(
                    {
                        "id": cid,
                        "legs": leg_info,
                        "missing": missing,
                        "roi_bps": roi_bps,
                        "gross_roi_bps": gross_roi_bps,
                        "start_ccy": start_ccy,
                        "end_ccy": end_ccy,
                        "ts_detected": ts_detected,
                        "age_ms": age_ms,
                        "leg_fills_net": opp.get("leg_fills_net") if opp else None,
                        "leg_fills_gross": opp.get("leg_fills_gross") if opp else None,
                    }
                )
            return JSONResponse(data)

        @self.app.get("/cycles", response_class=HTMLResponse)
        async def cycles_page():
            return HTMLResponse(_HTML_CYCLES_PAGE)

    async def start(self) -> None:
        if nats is None:
            raise RuntimeError("nats-py is required for the dashboard consumer")
        # Load cycles from redis if available
        if redis is not None:
            try:
                self._redis = redis.from_url(self.redis_url, decode_responses=True)
                await self._load_cycles_from_redis()
            except Exception:
                self._redis = None

        self._nc = await connect_nats_with_retry(self.nats_url)
        self._sub = await self._nc.subscribe(self.snapshot_subject, cb=self._on_snapshot)
        if self.opp_subject:
            self._opp_sub = await self._nc.subscribe(self.opp_subject, cb=self._on_opp)
        if self.cycles_subject:
            self._cycles_sub = await self._nc.subscribe(self.cycles_subject, cb=self._on_cycles)
        log.info("Depth dashboard subscribed to %s", self.snapshot_subject)

    async def stop(self) -> None:
        if self._sub:
            await self._sub.unsubscribe()
        if self._opp_sub:
            await self._opp_sub.unsubscribe()
        if self._cycles_sub:
            await self._cycles_sub.unsubscribe()
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
        self._nc = None
        self._sub = None
        self._opp_sub = None
        self._cycles_sub = None
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass
        self._redis = None

    async def _on_snapshot(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            if payload.get("source") != "ws_depth":
                return
            if self.symbols and payload.get("symbol", "").upper() not in self.symbols:
                return
            self.cache.update(payload)
        except Exception:
            return

    async def _on_opp(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            payload["ts_received"] = int(time.time() * 1000)
            self.opps.append(payload)
            if len(self.opps) > 200:
                self.opps = self.opps[-200:]
            cid = payload.get("cycle_id")
            if cid:
                self.last_opp_by_cycle[cid] = payload
        except Exception:
            return

    async def _on_cycles(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            if isinstance(payload, list):
                self.cycles = payload
        except Exception:
            return

    async def _load_cycles_from_redis(self) -> None:
        if not self._redis:
            return
        try:
            raw = await self._redis.get(f"{self.redis_prefix}:cycles")
            if raw:
                self.cycles = json.loads(raw)
        except Exception:
            return


_HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>CEX Depth Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0b1021; color: #e7ecf3; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #222c45; text-align: left; }
    th { background: #14213d; position: sticky; top: 0; }
    tr:nth-child(even) { background: #11182f; }
    .stale { color: #ffb347; }
  </style>
</head>
<body>
  <h2>Depth Snapshots (md.depth)</h2>
  <div id="updated">Loading...</div>
  <table id="depth-grid">
    <thead>
      <tr><th>Symbol</th><th>Exchange</th><th>Top Bids</th><th>Top Asks</th><th>Depth</th><th>Age (ms)</th><th>Source</th></tr>
    </thead>
    <tbody></tbody>
  </table>

  <p><a href="/opps" style="color:#8dc5ff;">View Opportunities »</a></p>
  <p><a href="/cycles" style="color:#8dc5ff;">View Cycles »</a></p>

  <h2>Latest Opportunities (strategy.depth.opportunity)</h2>
  <div id="opp-updated">Loading...</div>
  <table id="opp-grid">
    <thead>
      <tr><th>ROI (bps)</th><th>Gross ROI (bps)</th><th>Cycle</th><th>Start</th><th>End</th><th>Detected ms</th></tr>
    </thead>
    <tbody></tbody>
  </table>
  <script>
    async function refreshDepth() {
      try {
        const res = await fetch('/api/depth');
        const rows = await res.json();
        const tbody = document.querySelector('#depth-grid tbody');
        if (!tbody) return;
        tbody.innerHTML = '';
        rows.forEach(r => {
          const tr = document.createElement('tr');
          const bids = r.bids?.slice(0, 5).map(l => `${l[0]} @ ${l[1]}`).join('<br/>') || '';
          const asks = r.asks?.slice(0, 5).map(l => `${l[0]} @ ${l[1]}`).join('<br/>') || '';
          const age = r.age_ms || 0;
          if (age > 5000) tr.classList.add('stale');
          tr.innerHTML = `
            <td>${r.symbol}</td>
            <td>${r.exchange}</td>
            <td>${bids}</td>
            <td>${asks}</td>
            <td>${r.depth ?? ''}</td>
            <td>${age}</td>
            <td>${r.source ?? ''}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refreshDepth();
    setInterval(refreshDepth, 1500);

    async function refreshOpps() {
      try {
        const res = await fetch('/api/opps');
        const rows = await res.json();
        const tbody = document.querySelector('#opp-grid tbody');
        if (!tbody) return;
        tbody.innerHTML = '';
        rows.slice().reverse().forEach(r => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${r.roi_bps ?? ''}</td>
            <td>${r.gross_roi_bps ?? ''}</td>
            <td>${r.cycle_id ?? ''}</td>
            <td>${r.notional_usd_start ?? ''}</td>
            <td>${r.notional_usd_end ?? ''}</td>
            <td>${r.ts_detected ?? ''}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('opp-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refreshOpps();
    setInterval(refreshOpps, 2000);
  </script>
</body>
</html>
"""

_HTML_OPPS_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>CEX Depth Opportunities</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0b1021; color: #e7ecf3; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #222c45; text-align: left; }
    th { background: #14213d; position: sticky; top: 0; }
    tr:nth-child(even) { background: #11182f; }
  </style>
</head>
<body>
  <h2>Latest Opportunities (strategy.depth.opportunity)</h2>
  <div id="opp-updated">Loading...</div>
  <table id="opp-grid">
    <thead>
      <tr><th>ROI (bps)</th><th>Gross ROI (bps)</th><th>Cycle</th><th>Start</th><th>End</th><th>Detected ms</th></tr>
    </thead>
    <tbody></tbody>
  </table>
  <p><a href="/" style="color:#8dc5ff;">« Back to Depth</a></p>
  <script>
    async function refreshOpps() {
      try {
        const res = await fetch('/api/opps');
        const rows = await res.json();
        const tbody = document.querySelector('#opp-grid tbody');
        if (!tbody) return;
        tbody.innerHTML = '';
        rows.slice().reverse().forEach(r => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${r.roi_bps ?? ''}</td>
            <td>${r.gross_roi_bps ?? ''}</td>
            <td>${r.cycle_id ?? ''}</td>
            <td>${r.notional_usd_start ?? ''}</td>
            <td>${r.notional_usd_end ?? ''}</td>
            <td>${r.ts_detected ?? ''}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('opp-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refreshOpps();
    setInterval(refreshOpps, 2000);
  </script>
</body>
</html>
"""

_HTML_CYCLES_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>CEX Depth Cycles</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0b1021; color: #e7ecf3; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #222c45; text-align: left; vertical-align: top; }
    th { background: #14213d; position: sticky; top: 0; }
    tr:nth-child(even) { background: #11182f; }
    .missing { color: #ff6666; }
    .ok { color: #9af7a2; }
  </style>
</head>
<body>
  <h2>Cycles (top-of-book snapshot)</h2>
  <div id="updated">Loading...</div>
  <table id="cycle-grid">
    <thead>
      <tr><th>Cycle ID</th><th>ROI (bps)</th><th>Gross ROI (bps)</th><th>Age (ms)</th><th>Status</th><th>Legs (px / fill / avail / fee)</th></tr>
    </thead>
    <tbody></tbody>
  </table>
  <script>
    async function refreshCycles() {
      try {
        const res = await fetch('/api/cycles');
        const rows = await res.json();
        const tbody = document.querySelector('#cycle-grid tbody');
        tbody.innerHTML = '';
        rows.forEach(r => {
          const tr = document.createElement('tr');
          const legsSrc = r.leg_fills_net || r.legs || [];
          const legsTxt = legsSrc.map(l => {
            const fee = l.fee_bps !== undefined ? ` fee ${Number(l.fee_bps).toFixed(2)}bps` : '';
            const fill = l.fill_sz !== undefined ? ` fill ${l.fill_sz}` : '';
            const avail = l.avail_sz !== undefined ? `/${l.avail_sz}` : '';
            const px = l.px !== undefined ? l.px : l.price;
            return `${l.side} ${l.sym} @ ${px ?? ''}${fill}${avail}${fee}`;
          }).join('<br/>');
          tr.innerHTML = `
            <td>${r.id ?? ''}</td>
            <td>${r.roi_bps !== null && r.roi_bps !== undefined ? Number(r.roi_bps).toFixed(3) : ''}</td>
            <td>${r.gross_roi_bps !== null && r.gross_roi_bps !== undefined ? Number(r.gross_roi_bps).toFixed(3) : ''}</td>
            <td>${r.age_ms ?? ''}</td>
            <td class="${r.missing ? 'missing' : 'ok'}">${r.missing ? 'missing book' : 'ok'}</td>
            <td>${legsTxt}</td>
          `;
          tbody.appendChild(tr);
        });
        document.getElementById('updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      } catch (e) {
        console.error(e);
      }
    }
    refreshCycles();
    setInterval(refreshCycles, 2000);
  </script>
</body>
</html>
"""
