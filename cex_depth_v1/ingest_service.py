from __future__ import annotations

from cex_triarb_v1.ingest.runner import IngestService


class DepthIngestService(IngestService):
    """Depth-only ingest: disables ticker publishing."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("depth_enabled", True)
        kwargs.setdefault("rest_enabled", False)
        # Passthrough for batch size to avoid unexpected kwarg errors.
        kwargs.setdefault("coinbase_batch_size", kwargs.get("coinbase_batch_size", 40))
        super().__init__(*args, **kwargs)

    async def _handle_ticker(self, exchange: str, ticker: object) -> None:  # type: ignore[override]
        # Depth-first stack drops L1 ticker snapshots.
        return None
