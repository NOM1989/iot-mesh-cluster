"""Connectivity → LED matrix overlay logic.

Monitors the three connection stages for the local Pi by subscribing to:
  sensors.<host>.sensehat.network.internet   (from sensehat.py network poller)
  sensors.<host>.sensehat.network.rpi_rpi    (from sensehat.py network poller)
  sensors.<host>.msr2.online                 (from mmwave.py ESPHome bridge)

When any stage is not True, publishes a set_overlay command that places a
2-pixel indicator in the bottom row of the LED matrix (row 7) for each
failing stage — without disturbing the main display effect. When all three
stages are True, clears the overlay.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal

import nats

from bare_metal.common import load_runtime_config
from bare_metal.display.status_renderer import build_overlay_cells

log = logging.getLogger(__name__)


def _set_overlay_payload(connectivity: dict[str, bool | None]) -> bytes:
    cells = build_overlay_cells(connectivity)
    return json.dumps(
        {"effect": "set_overlay", "cells": cells},
        separators=(",", ":"),
    ).encode()


def _clear_overlay_payload() -> bytes:
    return json.dumps({"effect": "clear_overlay"}, separators=(",", ":")).encode()


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
        if all(v is True for v in connectivity.values()):
            log.info("All stages connected — clearing overlay")
            await nc.publish(matrix_subject, _clear_overlay_payload())
        else:
            log.debug("Connectivity state: %s", connectivity)
            await nc.publish(matrix_subject, _set_overlay_payload(connectivity))

    async def handle_network(msg: nats.aio.msg.Msg) -> None:
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

    # Publish initial overlay state immediately (all stages unknown → show overlay).
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
