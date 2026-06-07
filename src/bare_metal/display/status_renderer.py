"""Pixel renderer for the connectivity status_bars display.

Shared between boot_status.py (direct Sense HAT) and the matrix.py
status_bars effect (NATS-driven). Keeps the visual logic in one place.

Layout per row (8 columns):
  cols 0-1: solid indicator block (identifies the stage)
  cols 2-7: left-to-right comet sweep using status colour
"""
from __future__ import annotations

_OFF = (0, 0, 0)

# Unique indicator block colours per stage row index.
STAGE_BASE_COLORS: dict[int, tuple[int, int, int]] = {
    0: (0,   80,  200),  # internet  - blue
    1: (180, 0,   180),  # msr2      - magenta
    2: (0,   180, 180),  # rpi_rpi   - cyan
}

# Status colours used by callers.
COLOR_PENDING = (255, 140, 0)   # orange
COLOR_CONNECTED = (0, 200, 0)   # green
COLOR_FAILED = (180, 0, 0)      # red


def render_frame(
    stages: dict[int, dict],
    sweep_pos: int,
) -> list[tuple[int, int, int]]:
    """Build the 64-pixel list for the 8×8 matrix.

    stages: {row_index: {"indicator": (r,g,b), "status": (r,g,b)}}
    sweep_pos: comet head position within the 6-pixel bar (0–5).
    Rows not present in stages are left black (off).
    """
    pixels: list[tuple[int, int, int]] = [_OFF] * 64

    for row_idx, stage in stages.items():
        indicator: tuple[int, int, int] = tuple(stage["indicator"])  # type: ignore[assignment]
        status: tuple[int, int, int] = tuple(stage["status"])        # type: ignore[assignment]
        base = int(row_idx) * 8

        # Cols 0–1: solid indicator block
        pixels[base + 0] = indicator
        pixels[base + 1] = indicator

        # Cols 2–7: comet sweep
        for bar_col in range(6):
            if bar_col > sweep_pos:
                # Ahead of the comet head — off
                pixels[base + 2 + bar_col] = _OFF
            else:
                behind = sweep_pos - bar_col
                if behind == 0:
                    brightness = 1.0
                elif behind == 1:
                    brightness = 0.35
                elif behind == 2:
                    brightness = 0.12
                else:
                    brightness = 0.0
                r = min(255, int(status[0] * brightness))
                g = min(255, int(status[1] * brightness))
                b = min(255, int(status[2] * brightness))
                pixels[base + 2 + bar_col] = (r, g, b)

    return pixels
