"""Apollo MSR-2 (ESPHome) → NATS bridge.

Connects to the local MSR-2 over the Pi's hotspot via aioesphomeapi,
subscribes to entity state updates, and republishes each one onto a
NATS subject according to the entity map in /etc/iot-mesh/config.json.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from aioesphomeapi import APIClient, EntityInfo, EntityState, ReconnectLogic
from zeroconf.asyncio import AsyncZeroconf

from bare_metal.common import MeshPublisher, load_runtime_config

log = logging.getLogger(__name__)

DEVICE = "msr2"
ESPHOME_API_PORT = 6053

# Entities behind the engineering_mode switch; we only republish them
# when radar_engineering_mode is on.
ENG_GATE_PREFIXES: tuple[str, ...] = (
    "g0_", "g1_", "g2_", "g3_", "g4_", "g5_", "g6_", "g7_", "g8_",
    "radar_move_energy", "radar_still_energy",
)

CONFIG_ENTITY_TYPES = {"NumberInfo", "SwitchInfo", "SelectInfo"}


class MmwaveBridge:
    def __init__(
        self,
        pub: MeshPublisher,
        cfg_data: dict[str, Any],
        mmwave_host: str,
    ) -> None:
        self._pub = pub
        self._mmwave_host = mmwave_host
        entity_map = cfg_data["msr2_entity_map"]
        self._sensors: dict[str, dict] = entity_map.get("sensors", {})
        self._info: dict[str, dict] = entity_map.get("info", {})
        self._skipped: set[str] = set(entity_map.get("skipped", []))
        self._entities_by_key: dict[int, EntityInfo] = {}
        self._engineering_mode = False

    def _classify(self, ent: EntityInfo) -> tuple[str, str, str | None, str | None] | None:
        name = ent.object_id
        if name in self._skipped:
            return None
        if name in self._sensors:
            spec = self._sensors[name]
            return ("sensors", spec["metric"], spec.get("unit"), spec.get("source"))
        if name in self._info:
            spec = self._info[name]
            return ("info", spec["metric"], None, spec.get("source"))
        if any(name.startswith(p) for p in ENG_GATE_PREFIXES):
            if not self._engineering_mode:
                return None
            return ("sensors", f"gates.{name}", None, None)
        if type(ent).__name__ in CONFIG_ENTITY_TYPES:
            return ("config", name, None, None)
        return None

    def _on_state_sync(self, state: EntityState) -> None:
        # aioesphomeapi calls this synchronously from the loop thread.
        asyncio.create_task(self._on_state_async(state))

    async def _on_state_async(self, state: EntityState) -> None:
        ent = self._entities_by_key.get(state.key)
        if ent is None:
            return

        if ent.object_id == "radar_engineering_mode":
            self._engineering_mode = bool(getattr(state, "state", False))

        if getattr(state, "missing_state", False):
            return

        target = self._classify(ent)
        if target is None:
            return
        tree, metric, unit, source = target
        value = getattr(state, "state", None)
        if value is None:
            return
        try:
            await self._pub.publish(tree, metric, value, unit=unit, source=source)
        except Exception:
            log.exception("publish failed: %s.%s", tree, metric)

    async def run(self) -> None:
        # Hotspot is isolated to this Pi + MSR-2, so no encryption/password.
        client = APIClient(
            address=self._mmwave_host,
            port=ESPHOME_API_PORT,
            password="",
            client_info=f"iot-mesh-cluster/{DEVICE}",
        )
        zc = AsyncZeroconf()

        async def on_connect() -> None:
            log.info("ESPHome connected: %s", self._mmwave_host)
            entities, _ = await client.list_entities_services()
            self._entities_by_key = {e.key: e for e in entities}
            log.info("Discovered %d entities", len(entities))
            client.subscribe_states(self._on_state_sync)

        async def on_disconnect(expected: bool) -> None:
            log.warning("ESPHome disconnected (expected=%s)", expected)

        reconnect = ReconnectLogic(
            client=client,
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            zeroconf_instance=zc.zeroconf,
            name=DEVICE,
        )
        await reconnect.start()

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()

        await reconnect.stop()
        await zc.async_close()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = load_runtime_config("MMWAVE_SEED", "MMWAVE_HOST")

    pub = MeshPublisher(cfg.host_label, DEVICE, cfg.nats_url, cfg.seed_path)
    await pub.connect()

    bridge = MmwaveBridge(pub, cfg.data, cfg.extras["MMWAVE_HOST"])
    try:
        await bridge.run()
    finally:
        await pub.close()
        log.info("MSR-2 publisher stopped")


if __name__ == "__main__":
    asyncio.run(main())
