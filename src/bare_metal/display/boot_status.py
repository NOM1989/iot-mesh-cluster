"""Boot connectivity status display.

Runs as a one-shot systemd service before sensehat.service starts. Drives
the 8×8 LED matrix directly (no NATS) to show pending connectivity stages
as a compact overlay in row 7 (the bottom row):

  For each unresolved stage: [indicator pixel][pulsing status pixel]
    Internet  — blue   indicator (col 0/1)
    MSR2      — magenta indicator (col 2/3)
    RPi↔RPi   — cyan   indicator (col 4/5)

Rows 0–6 stay black. Stages clear from the overlay when they connect.
Exits once all three stages are resolved (or after TIMEOUT_S), leaving the
matrix clear for sensehat.service to take over.
"""
from __future__ import annotations

import json
import logging
import math
import os
import socket
import threading
import time

from sense_hat import SenseHat

from bare_metal.display.status_renderer import (
    COLOR_PENDING,
    STAGE_BASE_COLORS,
)

log = logging.getLogger(__name__)

TIMEOUT_S    = 300    # 5 minutes total
PROBE_INTERVAL_S = 2  # seconds between TCP probe rounds
FRAME_DELAY_S    = 0.05   # ~20 fps for smooth sine pulse

_OFF = (0, 0, 0)
_STAGE_KEYS = ["internet", "msr2", "rpi_rpi"]


def _tcp_reachable(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except OSError:
        return False


def _probe_loop(
    msr2_ip: str,
    peer_ips: list[str],
    state: dict[str, str],   # "pending" | "connected" | "failed"
    lock: threading.Lock,
    done: threading.Event,
) -> None:
    """Background thread: probe TCP stages and update shared state."""
    targets = {
        "internet": ("8.8.8.8", 53),
        "msr2":     (msr2_ip, 6053),
        "rpi_rpi":  (peer_ips[0] if peer_ips else None, 4222),
    }

    while not done.is_set():
        for key, (host, port) in targets.items():
            with lock:
                if state[key] != "pending":
                    continue  # already resolved

            if host is None:
                with lock:
                    state[key] = "failed"
                log.warning("No peer IP configured for %s; marking failed", key)
                continue

            if _tcp_reachable(host, port):
                with lock:
                    state[key] = "connected"
                log.info("Stage %s connected (%s:%s)", key, host, port)

        with lock:
            all_resolved = all(s != "pending" for s in state.values())
        if all_resolved:
            break

        done.wait(timeout=PROBE_INTERVAL_S)


def _render_overlay(state: dict[str, str], t: float) -> list:
    """64-pixel list: black background, row 7 shows failing stages."""
    pixels: list = [_OFF] * 64
    brightness = (math.sin(2 * math.pi * t / 2.0) + 1) / 2  # 2-second period
    col = 0
    for i, key in enumerate(_STAGE_KEYS):
        if state[key] == "connected":
            continue
        pixels[7 * 8 + col]     = STAGE_BASE_COLORS[i]
        pixels[7 * 8 + col + 1] = tuple(int(c * brightness) for c in COLOR_PENDING)
        col += 2
    return pixels


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config_path = os.environ.get("CONFIG_PATH", "/etc/iot-mesh/config.json")
    try:
        with open(config_path) as f:
            config = json.load(f)
    except OSError:
        log.warning("Config not found at %s; using defaults", config_path)
        config = {}

    msr2_ip: str = config.get("msr2_static_ip", "192.168.10.10")
    peer_ips: list[str] = config.get("peer_tailscale_ips", [])

    log.info("Boot status: msr2=%s peers=%s timeout=%ds", msr2_ip, peer_ips, TIMEOUT_S)

    state: dict[str, str] = {k: "pending" for k in _STAGE_KEYS}
    lock = threading.Lock()
    done = threading.Event()

    probe_thread = threading.Thread(
        target=_probe_loop,
        args=(msr2_ip, peer_ips, state, lock, done),
        daemon=True,
        name="boot-probe",
    )

    sense = SenseHat()
    probe_thread.start()

    start = time.monotonic()

    try:
        while True:
            elapsed = time.monotonic() - start

            if elapsed >= TIMEOUT_S:
                with lock:
                    for key in _STAGE_KEYS:
                        if state[key] == "pending":
                            state[key] = "failed"
                            log.warning("Stage %s timed out", key)
                done.set()
                break

            with lock:
                snapshot = dict(state)

            pixels = _render_overlay(snapshot, elapsed)
            sense.set_pixels([[r, g, b] for r, g, b in pixels])

            if all(s != "pending" for s in snapshot.values()):
                done.set()
                break

            time.sleep(FRAME_DELAY_S)

        # Hold the final state briefly so it's visible.
        time.sleep(2)

    finally:
        sense.clear()
        log.info("Boot status complete: %s", state)


if __name__ == "__main__":
    main()
