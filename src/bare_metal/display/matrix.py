"""SenseHAT 8×8 LED matrix effect engine + NATS command subscriber.

Subscribes to command.<host>.sensehat.> and drives the LED matrix with
named effects. A new command cancels any running effect before starting
the next one.

Message format (UTF-8 JSON):
    {"effect": "pulse"}
    {"effect": "solid",       "color": [255, 0, 0]}
    {"effect": "pulse",       "color": [0, 128, 255], "speed": 1.0}
    {"effect": "flash",       "color": [255, 255, 0], "count": 3, "speed": 1.0}
    {"effect": "rainbow",     "speed": 1.0}
    {"effect": "off"}
    {"effect": "set_overlay", "cells": [{"row": 7, "col": 0, "color": [R,G,B], "pulse": false}, ...]}
    {"effect": "clear_overlay"}

set_overlay and clear_overlay do NOT cancel the running base effect — they
composite status indicators on top of it each frame.

A bare string body ("pulse") is accepted as shorthand for {"effect": "pulse"}.
"""

from __future__ import annotations

import asyncio
import colorsys
import json
import logging
import math
import threading
from dataclasses import dataclass
from typing import Any

from sense_hat import SenseHat

log = logging.getLogger(__name__)

_WHITE: tuple[int, int, int] = (255, 255, 255)
_OFF:   tuple[int, int, int] = (0,   0,   0)

_OVERLAY_PULSE_PERIOD = 2.0   # seconds for one brightness sine cycle


def _clamp(v: float) -> int:
    return max(0, min(255, int(round(v))))


def _scale(color: tuple[int, int, int], brightness: float) -> tuple[int, int, int]:
    return (
        _clamp(color[0] * brightness),
        _clamp(color[1] * brightness),
        _clamp(color[2] * brightness),
    )


def _parse_color(raw: Any, default: tuple[int, int, int]) -> tuple[int, int, int]:
    if isinstance(raw, (list, tuple)) and len(raw) == 3:
        return (int(raw[0]), int(raw[1]), int(raw[2]))
    return default


@dataclass(frozen=True)
class OverlayCell:
    row:   int
    col:   int
    color: tuple[int, int, int]
    pulse: bool   # True → brightness modulated by sine wave; False → solid


class MatrixController:
    def __init__(self, sense: SenseHat, loop: asyncio.AbstractEventLoop) -> None:
        self._sense = sense
        self._loop = loop
        self._task: asyncio.Task | None = None
        self._hw_lock = threading.Lock()
        self._overlay: list[OverlayCell] = []

    # ── overlay management ───────────────────────────────────────────────

    def _set_overlay(self, cells: list[dict]) -> None:
        parsed: list[OverlayCell] = []
        for c in cells:
            try:
                parsed.append(OverlayCell(
                    row=int(c["row"]),
                    col=int(c["col"]),
                    color=tuple(int(x) for x in c["color"]),  # type: ignore[arg-type]
                    pulse=bool(c.get("pulse", False)),
                ))
            except (KeyError, ValueError, TypeError):
                log.warning("Malformed overlay cell %r — skipping", c)
        self._overlay = parsed
        log.debug("Overlay set: %d cells", len(parsed))

    def _apply_overlay(self, pixels: list, t: float) -> None:
        """Composite overlay cells onto pixels in place. t = elapsed seconds."""
        overlay = self._overlay
        if not overlay:
            return
        brightness = (math.sin(2 * math.pi * t / _OVERLAY_PULSE_PERIOD) + 1) / 2
        for cell in overlay:
            idx = cell.row * 8 + cell.col
            if not (0 <= idx < 64):
                continue
            pixels[idx] = _scale(cell.color, brightness) if cell.pulse else cell.color

    # ── public interface ─────────────────────────────────────────────────

    async def run(self, effect: str, params: dict[str, Any]) -> None:
        # Overlay commands update state without disturbing the running effect.
        if effect == "set_overlay":
            self._set_overlay(params.get("cells", []))
            return
        if effect == "clear_overlay":
            self._overlay = []
            log.debug("Overlay cleared")
            return

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

    # ── hardware helpers ─────────────────────────────────────────────────

    async def _set_pixels(self, pixels: list[tuple[int, int, int]]) -> None:
        pixels_ll = [[r, g, b] for r, g, b in pixels]
        def _write() -> None:
            with self._hw_lock:
                self._sense.set_pixels(pixels_ll)
        await self._loop.run_in_executor(None, _write)

    async def _clear(self) -> None:
        def _write() -> None:
            with self._hw_lock:
                self._sense.clear()
        await self._loop.run_in_executor(None, _write)

    # ── effects ──────────────────────────────────────────────────────────

    async def _effect_off(self, _params: dict) -> None:
        """Black background; keeps running so overlay can animate."""
        step = 0.05
        t = 0.0
        try:
            while True:
                if self._overlay:
                    pixels = [_OFF] * 64
                    self._apply_overlay(pixels, t)
                    await self._set_pixels(pixels)
                    await asyncio.sleep(step)
                    t += step
                else:
                    await self._clear()
                    await asyncio.sleep(1.0)
                    t += 1.0
        except asyncio.CancelledError:
            raise

    async def _effect_solid(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        step = 0.05
        t = 0.0
        try:
            while True:
                pixels = [color] * 64
                self._apply_overlay(pixels, t)
                await self._set_pixels(pixels)
                await asyncio.sleep(step)
                t += step
        except asyncio.CancelledError:
            raise

    async def _effect_pulse(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        speed = float(params.get("speed", 0.5))
        period = 1.0 / max(speed, 0.05)
        step = 0.02
        t = 0.0
        try:
            while True:
                brightness = (math.sin(2 * math.pi * t / period) + 1) / 2
                pixels = [_scale(color, brightness)] * 64
                self._apply_overlay(pixels, t)
                await self._set_pixels(pixels)
                await asyncio.sleep(step)
                t += step
        except asyncio.CancelledError:
            raise

    async def _effect_flash(self, params: dict) -> None:
        color = _parse_color(params.get("color"), _WHITE)
        count = int(params.get("count", 3))
        speed = float(params.get("speed", 1.0))
        half = 0.5 / max(speed, 0.05)
        t = 0.0
        try:
            for _ in range(count):
                pixels = [color] * 64
                self._apply_overlay(pixels, t)
                await self._set_pixels(pixels)
                await asyncio.sleep(half)
                t += half
                pixels = [_OFF] * 64
                self._apply_overlay(pixels, t)
                await self._set_pixels(pixels)
                await asyncio.sleep(half)
                t += half
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
                rgb = (_clamp(r * 255), _clamp(g * 255), _clamp(b * 255))
                pixels = [rgb] * 64
                self._apply_overlay(pixels, t)
                await self._set_pixels(pixels)
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
