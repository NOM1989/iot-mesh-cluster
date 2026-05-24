"""Message envelope for sensor → NATS publishing."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """ISO-8601 UTC timestamp with millisecond precision and a trailing 'Z'."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_message(
    host: str,
    device: str,
    metric: str,
    value: Any,
    unit: str | None = None,
    source: str | None = None,
) -> bytes:
    """Build a JSON-encoded sensor reading.

    `value` is a scalar (number / bool) for most metrics, or an object like
    {"x": ..., "y": ..., "z": ...} for IMU-style vector readings.
    """
    msg: dict[str, Any] = {
        "ts": utc_now_iso(),
        "host": host,
        "device": device,
        "metric": metric,
        "value": value,
    }
    if unit is not None:
        msg["unit"] = unit
    if source is not None:
        msg["source"] = source
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")
