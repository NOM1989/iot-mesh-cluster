"""Async NATS publisher used by every bare-metal sensor script.

One MeshPublisher per (device, host). Connects to the local NATS broker
with this service's NKey seed and publishes structured messages to
`<tree>.<host>.<device>.<metric>` subjects.
"""

from __future__ import annotations

import logging
from typing import Any

import nats
from nats.aio.client import Client as NATSClient

from .schema import build_message

log = logging.getLogger(__name__)


class MeshPublisher:
    def __init__(
        self,
        host_label: str,
        device: str,
        nats_url: str,
        seed_path: str,
    ) -> None:
        self.host_label = host_label
        self.device = device
        self.nats_url = nats_url
        self.seed_path = seed_path
        self._nc: NATSClient | None = None

    async def connect(self) -> None:
        self._nc = await nats.connect(
            servers=[self.nats_url],
            nkeys_seed=self.seed_path,
            name=f"{self.device}@{self.host_label}",
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            ping_interval=20,
            error_cb=self._on_error,
            disconnected_cb=self._on_disconnected,
            reconnected_cb=self._on_reconnected,
        )
        log.info("Connected to %s as %s@%s", self.nats_url, self.device, self.host_label)

    async def close(self) -> None:
        if self._nc is not None and self._nc.is_connected:
            await self._nc.drain()
            self._nc = None

    async def publish(
        self,
        tree: str,
        metric: str,
        value: Any,
        unit: str | None = None,
        source: str | None = None,
    ) -> None:
        """Publish a reading to `<tree>.<host_label>.<device>.<metric>`."""
        if self._nc is None:
            raise RuntimeError("MeshPublisher not connected")
        subject = f"{tree}.{self.host_label}.{self.device}.{metric}"
        payload = build_message(
            host=self.host_label,
            device=self.device,
            metric=metric,
            value=value,
            unit=unit,
            source=source,
        )
        await self._nc.publish(subject, payload)

    async def _on_error(self, e: Exception) -> None:
        log.error("NATS error: %s", e)

    async def _on_disconnected(self) -> None:
        log.warning("NATS disconnected")

    async def _on_reconnected(self) -> None:
        log.info("NATS reconnected to %s", self._nc.connected_url.netloc if self._nc else "?")
