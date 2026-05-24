"""Runtime configuration loader.

Services read static settings from environment variables (set by the
systemd EnvironmentFile at /etc/default/iot-mesh) and dynamic settings
(entity maps, sensor lists) from a JSON file rendered by Ansible at
/etc/iot-mesh/config.json.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RuntimeConfig:
    host_label: str
    nats_url: str
    seed_path: str
    config_path: Path
    extras: dict[str, str]
    data: dict[str, Any]

    def section(self, key: str) -> Any:
        return self.data.get(key)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable {name} is not set")
    return value


def load_runtime_config(seed_env: str, *extra_env: str) -> RuntimeConfig:
    """Load env-driven settings plus the JSON config blob.

    `seed_env` is the env var name pointing at this service's NKey seed
    file (e.g. SENSEHAT_SEED). `extra_env` lists any additional env vars
    the caller needs (returned in `extras`).
    """
    host_label = _require_env("HOST_LABEL")
    nats_url = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    seed_path = _require_env(seed_env)
    config_path = Path(os.environ.get("CONFIG_PATH", "/etc/iot-mesh/config.json"))
    data = json.loads(config_path.read_text())
    extras = {name: _require_env(name) for name in extra_env}
    return RuntimeConfig(
        host_label=host_label,
        nats_url=nats_url,
        seed_path=seed_path,
        config_path=config_path,
        extras=extras,
        data=data,
    )
