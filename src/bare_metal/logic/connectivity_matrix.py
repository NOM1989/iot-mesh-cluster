"""Connectivity → LED matrix logic handler.

Monitors the three connection stages for the local Pi by subscribing to:
  sensors.<host>.sensehat.network.internet   (from sensehat.py network poller)
  sensors.<host>.sensehat.network.rpi_rpi    (from sensehat.py network poller)
  sensors.<host>.msr2.online                 (from mmwave.py ESPHome bridge)

When any stage is not yet known or is False, publishes a status_bars command
to the local LED matrix showing the current connectivity state. When all
three stages are True, publishes {"effect": "off"} to yield the display back
to the presence-detection logic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any

import nats

from bare_metal.common import load_runtime_config
from bare_metal.display.status_renderer import (
    COLOR_CONNECTED,
    COLOR_PENDING,
    STAGE_BASE_COLORS,
)

log = logging.getLogger(__name__)

# Row index → connectivity key mapping (matches status_renderer row layout).
_STAGE_ROWS: dict[int, str] = {
    0: "internet",
    1: "msr2",
    2: "rpi_rpi",
}


def _status_bars_payload(connectivity: dict[str, bool | None]) -> bytes:
    stages: dict[str, Any] = {}
    for row_idx, key in _STAGE_ROWS.items():
        connected = connectivity.get(key)
        status_color = list(COLOR_CONNECTED) if connected else list(COLOR_PENDING)
        stages[str(row_idx)] = {
            "indicator": list(STAGE_BASE_COLORS[row_idx]),
            "status":    status_color,
        }
    return json.dumps(
        {"effect": "status_bars", "stages": stages},
        separators=(",", ":"),
    ).encode()


def _off_payload() -> bytes:
    return json.dumps({"effect": "off"}, separators=(",", ":")).encode()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_runtime_config("LOGIC_SEED")
    host = cfg.host_label
    matrix_subject = f"command.{host}.sensehat.matrix"

    # None = not yet heard from; bool = last known state.
    connectivity: dict[str, bool | None] = {
        "internet": None,
        "msr2":     None,
        "rpi_rpi":  None,
    }

    log.info("Starting connectivity→matrix logic for %s", host)

    async def _on_error(e: Exception) -> None:
        log.error("NATS error: %s", e)

    async def _on_disconnected() -> None:
        log.warning("NATS disconnected")

    async def _on_reconnected() -> None:
        log.info("NATS reconnected")

    nc = await nats.connect(
        servers=[cfg.nats_url],
        nkeys_seed=cfg.seed_path,
        name=f"logic-connectivity@{host}",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
        ping_interval=20,
        error_cb=_on_error,
        disconnected_cb=_on_disconnected,
        reconnected_cb=_on_reconnected,
    )

    async def _update_matrix() -> None:
        all_connected = all(v is True for v in connectivity.values())
        if all_connected:
            log.info("All stages connected — clearing matrix")
            await nc.publish(matrix_subject, _off_payload())
        else:
            log.debug("Connectivity state: %s", connectivity)
            await nc.publish(matrix_subject, _status_bars_payload(connectivity))

    async def handle_network(msg: nats.aio.msg.Msg) -> None:
        # Subject: sensors.<host>.sensehat.network.<metric>
        parts = msg.subject.split(".")
        if len(parts) < 5:
            return
        metric = parts[-1]  # "internet" or "rpi_rpi"
        if metric not in connectivity:
            return
        try:
            body = json.loads(msg.data)
            value = bool(body.get("value", False))
        except (ValueError, KeyError):
            log.warning("Unparseable network message on %s", msg.subject)
            return

        prev = connectivity.get(metric)
        connectivity[metric] = value
        if value != prev:
            log.info("network.%s changed: %s → %s", metric, prev, value)
            await _update_matrix()

    async def handle_msr2_online(msg: nats.aio.msg.Msg) -> None:
        try:
            body = json.loads(msg.data)
            value = bool(body.get("value", False))
        except (ValueError, KeyError):
            log.warning("Unparseable msr2.online message on %s", msg.subject)
            return

        prev = connectivity.get("msr2")
        connectivity["msr2"] = value
        if value != prev:
            log.info("msr2.online changed: %s → %s", prev, value)
            await _update_matrix()

    network_sub = await nc.subscribe(
        f"sensors.{host}.sensehat.network.*", cb=handle_network
    )
    msr2_sub = await nc.subscribe(
        f"sensors.{host}.msr2.online", cb=handle_msr2_online
    )
    log.info(
        "Subscribed to sensors.%s.sensehat.network.* and sensors.%s.msr2.online",
        host, host,
    )

    # Show the initial unknown/pending state immediately.
    await _update_matrix()

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    await network_sub.unsubscribe()
    await msr2_sub.unsubscribe()
    await nc.drain()
    log.info("logic-connectivity stopped")


if __name__ == "__main__":
    asyncio.run(main())
