from __future__ import annotations

import asyncio
import json
import time
from typing import Dict, List, Optional

import fastapi
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import nats
except ImportError:  # pragma: no cover
    nats = None


class L1Cache:
    def __init__(self) -> None:
        # key: (exchange, symbol)
        self.data: Dict[tuple[str, str], dict] = {}

    def update(self, event: dict) -> None:
        bids = event.get("bids") or []
        asks = event.get("asks") or []
        best_bid = bids[0][0] if bids else None
        best_bid_size = bids[0][1] if bids else None
        best_ask = asks[0][0] if asks else None
        best_ask_size = asks[0][1] if asks else None
        symbol = event["symbol"]
        exchange = event["exchange"]
        self.data[(exchange, symbol)] = {
            "exchange": exchange,
            "symbol": symbol,
            "bid": best_bid,
            "bid_size": best_bid_size,
            "ask": best_ask,
            "ask_size": best_ask_size,
            "ts_event": event.get("ts_event"),
            "ts_ingest": event.get("ts_ingest"),
            "source": event.get("source"),
        }

    def snapshot(self) -> List[dict]:
        return sorted(
            [
                {
                    **row,
                    "mid": _mid(row.get("bid"), row.get("ask")),
                    "age_ms": max(0, int(time.time() * 1000) - (row.get("ts_ingest") or 0)),
                }
                for row in self.data.values()
            ],
            key=lambda r: (r["symbol"], r["exchange"]),
        )


class DepthCache:
    """Stores the most recent depth snapshot per (exchange, symbol)."""

    def __init__(self) -> None:
        self.data: Dict[tuple[str, str], dict] = {}

    def update(self, event: dict) -> None:
        exchange = event.get("exchange")
        symbol = event.get("symbol")
        if not exchange or not symbol:
            return
        self.data[(exchange, symbol)] = {
            "exchange": exchange,
            "symbol": symbol,
            "bids": event.get("bids", []),
            "asks": event.get("asks", []),
            "depth": event.get("depth"),
            "ts_event": event.get("ts_event"),
            "ts_ingest": event.get("ts_ingest"),
            "source": event.get("source"),
        }

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


def _mid(bid, ask):
    try:
        if bid is None or ask is None:
            return None
        return (float(bid) + float(ask)) / 2.0
    except Exception:
        return None


class DashboardService:
    def __init__(
        self,
        nats_url: str = "nats://127.0.0.1:4222",
        snapshot_subject: str = "md.snapshot",
        symbols: Optional[list[str]] = None,
    ) -> None:
        self.nats_url = nats_url
        self.snapshot_subject = snapshot_subject
        self.symbols = [s.upper() for s in symbols] if symbols else None
        self.cache = L1Cache()
        self.depth_cache = DepthCache()
        self._nc = None
        self._sub = None
        self.app = fastapi.FastAPI(title="CEX Tri-Arb Dashboard")
        self._mount_routes()

    def _mount_routes(self) -> None:
        @self.app.get("/healthz")
        async def healthz():
            return {"status": "ok", "count": len(self.cache.data)}

        @self.app.get("/api/snapshot")
        async def api_snapshot():
            return JSONResponse(self.cache.snapshot())

        @self.app.get("/api/depth")
        async def api_depth():
            return JSONResponse(self.depth_cache.snapshot())

        @self.app.get("/", response_class=HTMLResponse)
        async def index():
            return HTMLResponse(_HTML_PAGE)

    async def start(self) -> None:
        if nats is None:
            raise RuntimeError("nats-py is required for the dashboard consumer")
        self._nc = await nats.connect(self.nats_url)
        self._sub = await self._nc.subscribe(self.snapshot_subject, cb=self._on_snapshot)

    async def stop(self) -> None:
        if self._sub:
            await self._sub.unsubscribe()
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
        self._nc = None
        self._sub = None

    async def _on_snapshot(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            source = payload.get("source")
            if source == "ws_depth":
                if self.symbols and payload.get("symbol", "").upper() not in self.symbols:
                    return
                self.depth_cache.update(payload)
                # also feed L1 view with the top of the depth snapshot
                self.cache.update(payload)
                return
            if source != "ws":
                return
            if self.symbols and payload.get("symbol", "").upper() not in self.symbols:
                return
            self.cache.update(payload)
        except Exception:
            return


_HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>CEX Tri-Arb Dashboard</title>
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
  <h2>Live L1 Snapshot (md.snapshot)</h2>
  <div id="updated">Loading...</div>
  <table id="grid">
    <thead>
      <tr><th>Symbol</th><th>Exchange</th><th>Bid</th><th>Bid Qty</th><th>Ask</th><th>Ask Qty</th><th>Mid</th><th>Age (ms)</th><th>Source</th></tr>
    </thead>
    <tbody></tbody>
  </table>
  <script>
    async function refresh() {
      try {
        const res = await fetch('/api/snapshot');
        const rows = await res.json();
        const tbody = document.querySelector('#grid tbody');
        tbody.innerHTML = '';
        const now = Date.now();
        rows.forEach(r => {
          const tr = document.createElement('tr');
          const age = r.age_ms || 0;
          if (age > 5000) tr.classList.add('stale');
          tr.innerHTML = `
            <td>${r.symbol}</td>
            <td>${r.exchange}</td>
            <td>${r.bid ?? ''}</td>
            <td>${r.bid_size ?? ''}</td>
            <td>${r.ask ?? ''}</td>
            <td>${r.ask_size ?? ''}</td>
            <td>${r.mid ?? ''}</td>
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
    refresh();
    setInterval(refresh, 1500);

    // Depth table
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
      } catch (e) {
        console.error(e);
      }
    }
    refreshDepth();
    setInterval(refreshDepth, 2000);
  </script>

  <h2>Depth Snapshots (md.snapshot, source=ws_depth)</h2>
  <table id="depth-grid">
    <thead>
      <tr><th>Symbol</th><th>Exchange</th><th>Top Bids</th><th>Top Asks</th><th>Depth</th><th>Age (ms)</th><th>Source</th></tr>
    </thead>
    <tbody></tbody>
  </table>
</body>
</html>
"""
