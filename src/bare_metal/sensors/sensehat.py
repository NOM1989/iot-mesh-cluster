"""Sense HAT v1 publisher.

Polls all three sensor chips at a fixed rate and publishes each reading
to its own NATS subject. The synchronous sense_hat library calls run in
the default thread pool so the asyncio loop stays responsive.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from sense_hat import SenseHat

from bare_metal.common import MeshPublisher, build_message, load_runtime_config
from bare_metal.display.matrix import run_matrix_subscriber

log = logging.getLogger(__name__)

DEVICE = "sensehat"


def _read_all(sense: SenseHat) -> dict:
    return {
        "hts221_temperature": sense.get_temperature_from_humidity(),
        "hts221_humidity": sense.get_humidity(),
        "lps25h_temperature": sense.get_temperature_from_pressure(),
        "lps25h_pressure": sense.get_pressure(),
        "accelerometer": sense.get_accelerometer_raw(),
        "gyroscope": sense.get_gyroscope_raw(),
        "magnetometer": sense.get_compass_raw(),
        "orientation": sense.get_orientation_degrees(),
    }


def _xyz(d: dict) -> dict:
    return {"x": round(d["x"], 5), "y": round(d["y"], 5), "z": round(d["z"], 5)}


def _pry(d: dict) -> dict:
    return {"pitch": round(d["pitch"], 3), "roll": round(d["roll"], 3), "yaw": round(d["yaw"], 3)}


async def _publish_tick(pub: MeshPublisher, r: dict) -> None:
    await pub.publish("sensors", "temperature", round(float(r["hts221_temperature"]), 3),
                      unit="celsius", source="hts221")
    await pub.publish("sensors", "humidity", round(float(r["hts221_humidity"]), 3),
                      unit="percent", source="hts221")
    await pub.publish("sensors", "temperature", round(float(r["lps25h_temperature"]), 3),
                      unit="celsius", source="lps25h")
    await pub.publish("sensors", "pressure", round(float(r["lps25h_pressure"]), 3),
                      unit="hectopascal", source="lps25h")
    await pub.publish("sensors", "acceleration", _xyz(r["accelerometer"]),
                      unit="g", source="lsm9ds1")
    await pub.publish("sensors", "gyroscope", _xyz(r["gyroscope"]),
                      unit="rad_per_s", source="lsm9ds1")
    await pub.publish("sensors", "magnetometer", _xyz(r["magnetometer"]),
                      unit="microtesla", source="lsm9ds1")
    await pub.publish("sensors", "orientation", _pry(r["orientation"]),
                      unit="degree", source="lsm9ds1")


async def _tcp_reachable(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def _poll_network(
    pub: MeshPublisher,
    host_label: str,
    peer_ips: list[str],
    stop: asyncio.Event,
) -> None:
    """Poll internet + RPi-RPi reachability every 10s; publish as bool sensors."""
    INTERVAL = 10.0

    async def _probe_and_publish() -> None:
        nc = pub._nc
        if nc is None:
            return  # Not yet connected; skip this round.

        internet = await _tcp_reachable("8.8.8.8", 53)

        rpi_rpi = False
        for ip in peer_ips:
            if await _tcp_reachable(ip, 4222):
                rpi_rpi = True
                break

        for metric, value in (("internet", internet), ("rpi_rpi", rpi_rpi)):
            subject = f"sensors.{host_label}.sensehat.network.{metric}"
            payload = build_message(
                host=host_label, device="sensehat",
                metric=f"network.{metric}", value=value,
            )
            await nc.publish(subject, payload)
            log.debug("network.%s = %s", metric, value)

    while not stop.is_set():
        try:
            await _probe_and_publish()
        except Exception:
            log.exception("Network connectivity probe failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=INTERVAL)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = load_runtime_config(seed_env="SENSEHAT_SEED")
    publish_cfg = cfg.section("publish") or {}
    rate_hz = float(publish_cfg.get("sensehat_hz", 1))
    interval = 1.0 / rate_hz
    peer_ips: list[str] = cfg.section("peer_tailscale_ips") or []

    pub = MeshPublisher(cfg.host_label, DEVICE, cfg.nats_url, cfg.seed_path)
    await pub.connect()

    loop = asyncio.get_running_loop()
    sense = await loop.run_in_executor(None, SenseHat)
    log.info("Sense HAT initialised; publishing at %.2f Hz", rate_hz)

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    matrix_subject = f"command.{cfg.host_label}.sensehat.>"

    async def sensor_loop() -> None:
        while not stop.is_set():
            t0 = loop.time()
            readings = await loop.run_in_executor(None, _read_all, sense)
            await _publish_tick(pub, readings)
            elapsed = loop.time() - t0
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.0, interval - elapsed))
            except asyncio.TimeoutError:
                pass

    try:
        await asyncio.gather(
            sensor_loop(),
            run_matrix_subscriber(sense, pub._nc, matrix_subject, loop, stop),
            _poll_network(pub, cfg.host_label, peer_ips, stop),
        )
    finally:
        await pub.close()
        log.info("Sense HAT publisher stopped")


if __name__ == "__main__":
    asyncio.run(main())
