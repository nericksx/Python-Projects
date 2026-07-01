# src/pendo/util/time.py
from __future__ import annotations

from datetime import datetime, timezone


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)