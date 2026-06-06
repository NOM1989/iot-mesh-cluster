"""Presence → LED matrix automation.

Subscribes to sensors.*.msr2.presence on the NATS mesh. When presence is
detected on one Pi, the OTHER Pi's SenseHAT matrix runs a rainbow effect.
When presence clears, the matrix turns off after a configurable hold-off
delay (to absorb brief gaps in radar coverage).

Config (from /etc/iot-mesh/config.json):
  cluster_hosts:           list of host labels in the cluster (e.g. ["pi-viscous", "pi-wave"])
  logic.presence_hold_off_s: seconds to wait after presence clears before turning off (default 30)
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any

import nats

from bare_metal.common import load_runtime_config

log = logging.getLogger(__name__)

_MATRIX_SUBJECT = "command.{host}.sensehat.matrix"
_PRESENCE_SUBJECT = "sensors.*.msr2.presence"


def _payload(effect: str) -> bytes:
    return json.dumps({"effect": effect}, separators=(",", ":")).encode()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_runtime_config("LOGIC_SEED")
    cluster_hosts: list[str] = cfg.section("cluster_hosts") or []
    if not cluster_hosts:
        raise RuntimeError("cluster_hosts is missing or empty in config.json")

    logic_cfg: dict[str, Any] = cfg.section("logic") or {}
    hold_off = float(logic_cfg.get("presence_hold_off_s", 30))

    log.info(
        "Starting presence→matrix logic: hosts=%s hold_off=%.0fs",
        cluster_hosts,
        hold_off,
    )

    async def _on_error(e: Exception) -> None:
        log.error("NATS error: %s", e)

    async def _on_disconnected() -> None:
        log.warning("NATS disconnected")

    async def _on_reconnected() -> None:
        log.info("NATS reconnected")

    nc = await nats.connect(
        servers=[cfg.nats_url],
        nkeys_seed=cfg.seed_path,
        name=f"logic-presence@{cfg.host_label}",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
        ping_interval=20,
        error_cb=_on_error,
        disconnected_cb=_on_disconnected,
        reconnected_cb=_on_reconnected,
    )

    presence: dict[str, bool] = {}
    off_timers: dict[str, asyncio.Task] = {}

    async def handle(msg: nats.aio.msg.Msg) -> None:
        try:
            body = json.loads(msg.data)
        except (ValueError, KeyError):
            log.warning("Unparseable presence message on %s", msg.subject)
            return

        src_host: str = msg.subject.split(".")[1]
        value: bool = bool(body.get("value", False))

        if value == presence.get(src_host):
            return  # no state change, nothing to do

        presence[src_host] = value
        targets = [h for h in cluster_hosts if h != src_host]
        if not targets:
            log.warning("No target hosts for src=%s (cluster_hosts=%s)", src_host, cluster_hosts)
            return

        if value:
            # Presence detected — cancel any pending off-timer, start rainbow.
            timer = off_timers.pop(src_host, None)
            if timer and not timer.done():
                timer.cancel()
            log.info("Presence ON from %s → rainbow on %s", src_host, targets)
            for target in targets:
                await nc.publish(_MATRIX_SUBJECT.format(host=target), _payload("rainbow"))
        else:
            # Presence cleared — schedule delayed off.
            log.info(
                "Presence OFF from %s → off on %s in %.0fs",
                src_host, targets, hold_off,
            )

            async def _delayed_off(src: str, tgts: list[str]) -> None:
                try:
                    await asyncio.sleep(hold_off)
                    log.info("Hold-off expired for %s → off on %s", src, tgts)
                    for t in tgts:
                        await nc.publish(_MATRIX_SUBJECT.format(host=t), _payload("off"))
                except asyncio.CancelledError:
                    log.debug("Off-timer cancelled for %s (presence returned)", src)
                finally:
                    off_timers.pop(src, None)

            off_timers[src_host] = asyncio.ensure_future(_delayed_off(src_host, targets))

    sub = await nc.subscribe(_PRESENCE_SUBJECT, cb=handle)
    log.info("Subscribed to %s", _PRESENCE_SUBJECT)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    # Cancel any pending off-timers cleanly.
    for timer in off_timers.values():
        if not timer.done():
            timer.cancel()

    await sub.unsubscribe()
    await nc.drain()
    log.info("logic-presence stopped")


if __name__ == "__main__":
    asyncio.run(main())
