"""SenseHAT 8×8 LED matrix effect engine + NATS command subscriber.

Subscribes to command.<host>.sensehat.> and drives the LED matrix with
named effects. A new command cancels any running effect before starting
the next one.

Message format (UTF-8 JSON):
    {"effect": "pulse"}
    {"effect": "solid",   "color": [255, 0, 0]}
    {"effect": "pulse",   "color": [0, 128, 255], "speed": 1.0}
    {"effect": "flash",   "color": [255, 255, 0], "count": 3, "speed": 1.0}
    {"effect": "rainbow", "speed": 1.0}
    {"effect": "off"}

A bare string body ("pulse") is accepted as shorthand for {"effect": "pulse"}.
"""

from __future__ import annotations

import asyncio
import colorsys
import json
import logging
import math
from typing import Any

from sense_hat import SenseHat

log = logging.getLogger(__name__)

_WHITE = (255, 255, 255)


def _clamp(v: float) -> int:
    return max(0, min(255, int(round(v))))


def _scale(color: tuple[int, int, int], brightness: float) -> tuple[int, int, int]:
    return (_clamp(color[0] * brightness), _clamp(color[1] * brightness), _clamp(color[2] * brightness))


def _parse_color(raw: Any, default: tuple[int, int, int]) -> tuple[int, int, int]:
    if isinstance(raw, (list, tuple)) and len(raw) == 3:
        return (int(raw[0]), int(raw[1]), int(raw[2]))
    return default


class MatrixController:
    def __init__(self, sense: SenseHat, loop: asyncio.AbstractEventLoop) -> None:
        self._sense = sense
        self._loop = loop
        self._task: asyncio.Task | None = None

    async def run(self, effect: str, params: dict[str, Any]) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        handler = {
            "off":     self._effect_off,
            "solid":   self._effect_solid,
            "pulse":   self._effect_pulse,
            "flash":   self._effect_flash,
            "rainbow": self._effect_rainbow,
        }.get(effect)

        if handler is None:
            log.warning("unknown matrix effect %r — ignoring", effect)
            return

        async def _guarded() -> None:
            try:
                await handler(params)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Matrix effect %r raised an exception", effect)

        self._task = asyncio.ensure_future(_guarded())

    async def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ── helpers ──────────────────────────────────────────────────────────

    async def _set_all(self, color: tuple[int, int, int]) -> None:
        # set_pixels with an explicit list-of-lists is the safest form of the
        # sense_hat API — clear(tuple) behaves inconsistently across versions.
        r, g, b = color
        pixels = [[r, g, b]] * 64
        await self._loop.run_in_executor(None, self._sense.set_pixels, pixels)

    # ── effects ──────────────────────────────────────────────────────────

    async def _effect_off(self, params: dict) -> None:
        await self._loop.run_in_executor(None, self._sense.clear)

    async def _effect_solid(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        await self._set_all(color)

    async def _effect_pulse(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        speed = float(params.get("speed", 0.5))
        period = 1.0 / max(speed, 0.05)
        step = 0.02
        t = 0.0
        try:
            while True:
                brightness = (math.sin(2 * math.pi * t / period) + 1) / 2
                await self._set_all(_scale(color, brightness))
                await asyncio.sleep(step)
                t += step
        except asyncio.CancelledError:
            raise

    async def _effect_flash(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        count = int(params.get("count", 3))
        speed = float(params.get("speed", 1.0))
        half = 0.5 / max(speed, 0.05)
        try:
            for _ in range(count):
                await self._set_all(color)
                await asyncio.sleep(half)
                await self._loop.run_in_executor(None, self._sense.clear)
                await asyncio.sleep(half)
        except asyncio.CancelledError:
            raise

    async def _effect_rainbow(self, params: dict) -> None:
        speed = float(params.get("speed", 0.2))
        period = 1.0 / max(speed, 0.01)
        step = 0.05
        t = 0.0
        try:
            while True:
                hue = (t / period) % 1.0
                r, g, b = colorsys.hsv_to_rgb(hue, 1.0, 1.0)
                await self._set_all((_clamp(r * 255), _clamp(g * 255), _clamp(b * 255)))
                await asyncio.sleep(step)
                t += step
        except asyncio.CancelledError:
            raise


def _parse_command(data: bytes) -> tuple[str, dict[str, Any]]:
    """Return (effect_name, params) from raw message bytes."""
    text = data.decode("utf-8", errors="replace").strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text.lower(), {}
    if isinstance(payload, str):
        return payload.lower(), {}
    effect = str(payload.get("effect", "")).lower()
    params = {k: v for k, v in payload.items() if k != "effect"}
    return effect, params


async def run_matrix_subscriber(
    sense: SenseHat,
    nc,
    subject: str,
    loop: asyncio.AbstractEventLoop,
    stop: asyncio.Event,
) -> None:
    controller = MatrixController(sense, loop)
    log.info("Matrix subscriber listening on %s", subject)

    async def handle(msg) -> None:
        effect, params = _parse_command(msg.data)
        if not effect:
            log.warning("Received empty matrix command on %s", msg.subject)
            return
        log.info("Matrix command: effect=%r params=%r (subject=%s)", effect, params, msg.subject)
        await controller.run(effect, params)

    sub = await nc.subscribe(subject, cb=handle)
    try:
        await stop.wait()
    finally:
        await sub.unsubscribe()
        await controller.cancel()
        log.info("Matrix subscriber stopped")
