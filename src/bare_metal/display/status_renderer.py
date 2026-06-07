"""Connectivity status overlay rendering helpers.

Shared between boot_status.py (direct Sense HAT) and the NATS-driven
overlay system in matrix.py / connectivity_matrix.py.
"""
from __future__ import annotations

# Unique indicator block colours per stage row index.
STAGE_BASE_COLORS: dict[int, tuple[int, int, int]] = {
    0: (0,   80,  200),  # internet  - blue
    1: (140, 0,   191),  # msr2      - magenta
    2: (0,   255, 255),  # rpi_rpi   - turquoise
}

# Status colour for stages that are not yet confirmed connected.
COLOR_PENDING   = (180, 0, 0)   # red

# Maps stage row index → connectivity dict key.
STAGE_ROWS: dict[int, str] = {
    0: "internet",
    1: "msr2",
    2: "rpi_rpi",
}


def build_overlay_cells(connectivity: dict[str, bool | None]) -> list[dict]:
    """Return a cells list for set_overlay containing only failing/unknown stages.

    Each failing stage occupies 2 pixels in row 7:
      col 2n   — solid indicator colour
      col 2n+1 — pulsing orange

    Stages where value is True are excluded (connected = no overlay entry).
    None is treated the same as False (not confirmed connected).
    """
    cells: list[dict] = []
    col = 0
    for row_idx, key in STAGE_ROWS.items():
        if connectivity.get(key) is True:
            continue
        cells.append({
            "row":   7,
            "col":   col,
            "color": list(STAGE_BASE_COLORS[row_idx]),
            "pulse": False,
        })
        cells.append({
            "row":   7,
            "col":   col + 1,
            "color": list(COLOR_PENDING),
            "pulse": True,
        })
        col += 2
    return cells
