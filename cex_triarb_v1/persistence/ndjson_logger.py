from __future__ import annotations

import json
import pathlib
import time
from typing import Any, Dict, Iterable


class NdjsonLogger:
    def __init__(self, path: str = "logs/opportunities.ndjson") -> None:
        self.path = pathlib.Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record))
            fh.write("\n")

    def load(self) -> Iterable[Dict[str, Any]]:
        if not self.path.exists():
            return []
        out = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return out


def log_opportunity(logger: NdjsonLogger, payload: Dict[str, Any]) -> None:
    record = {"ts": int(time.time() * 1000), **payload}
    logger.append(record)
