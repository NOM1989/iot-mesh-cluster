"""Boot connectivity status display.

Runs as a one-shot systemd service before sensehat.service starts. Drives
the 8×8 LED matrix directly (no NATS) to show the three connection stages
as they come up:

  Row 0  Internet (TCP 8.8.8.8:53)
  Row 1  MSR2 radar (TCP <msr2_static_ip>:6053)
  Row 2  RPi↔RPi link (TCP <peer_tailscale_ip>:4222)

Each row shows a 2-pixel indicator block and a left-to-right comet sweep.
Sweep colour is orange while waiting, green when connected, red on timeout.

Exits once all three stages are resolved (or after TIMEOUT_S), leaving the
matrix clear for sensehat.service to take over.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time

from sense_hat import SenseHat

from bare_metal.display.status_renderer import (
    COLOR_CONNECTED,
    COLOR_FAILED,
    COLOR_PENDING,
    STAGE_BASE_COLORS,
    render_frame,
)

log = logging.getLogger(__name__)

TIMEOUT_S = 300        # 5 minutes total
PROBE_INTERVAL_S = 2   # seconds between TCP probe rounds
FRAME_DELAY_S = 0.15   # animation frame rate (~6.7 fps)

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
    state: dict[str, str],  # "pending" | "connected" | "failed"
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
                # No peer configured — mark as failed immediately.
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


def _build_stages(state: dict[str, str]) -> dict[int, dict]:
    status_color_map = {
        "pending":   COLOR_PENDING,
        "connected": COLOR_CONNECTED,
        "failed":    COLOR_FAILED,
    }
    return {
        i: {
            "indicator": STAGE_BASE_COLORS[i],
            "status":    status_color_map[state[_STAGE_KEYS[i]]],
        }
        for i in range(3)
    }


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
    sweep_pos = 0

    try:
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= TIMEOUT_S:
                # Mark remaining pending stages as failed.
                with lock:
                    for key in _STAGE_KEYS:
                        if state[key] == "pending":
                            state[key] = "failed"
                            log.warning("Stage %s timed out", key)
                done.set()
                break

            with lock:
                snapshot = dict(state)

            stages = _build_stages(snapshot)
            pixels = render_frame(stages, sweep_pos)
            sense.set_pixels(pixels)
            sweep_pos = (sweep_pos + 1) % 6

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
