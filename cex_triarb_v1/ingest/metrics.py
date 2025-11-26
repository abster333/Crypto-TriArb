from __future__ import annotations

try:
    from prometheus_client import Counter, Gauge  # type: ignore
except ImportError:  # lightweight no-op fallback
    class _NoOp:
        def __call__(self, *_, **__):
            return self

        def labels(self, *_, **__):
            return self

        def inc(self, *_, **__):
            return None

        def set(self, *_, **__):
            return None

    Counter = Gauge = _NoOp()  # type: ignore


WS_RECONNECTS = Counter("ws_reconnects_total", "Total websocket reconnect attempts", ["exchange"])
WS_MESSAGE_ERRORS = Counter("ws_message_errors_total", "Websocket messages that failed to parse", ["exchange"])
WS_LAST_TICK_TS = Gauge("ws_last_ticker_timestamp", "Unix timestamp (ms) of the last ticker event", ["exchange"])
WS_SNAPSHOTS_PUBLISHED = Counter("ws_snapshots_published_total", "Order book snapshots published from websocket ingests", ["exchange"])
WS_DEPTH_UPDATES = Counter("ws_depth_updates_total", "Depth snapshots/updates received from websocket feeds", ["exchange", "kind"])
WS_DEPTH_RESETS = Counter("ws_depth_resets_total", "Depth snapshots (full resets) received", ["exchange"])
WS_LAST_DEPTH_TS = Gauge("ws_last_depth_timestamp", "Unix timestamp (ms) of the last depth event per symbol", ["exchange", "symbol"])
WS_DEPTH_AGE_SECONDS = Gauge("ws_depth_age_seconds", "Age (seconds) between ws depth ts_event and ingest", ["exchange", "symbol"])
WS_DEPTH_STALE_EVENTS = Counter("ws_depth_stale_events_total", "Depth snapshots whose age exceeded the watchdog threshold", ["exchange"])
