# iot-mesh-cluster

A two-node Raspberry Pi mesh deployed across two locations``, linked by Tailscale, running a NATS message bus that fans sensor data between the nodes.

## Hardware (per node)

- Raspberry Pi 4
- USB WiFi adapter (upstream / `wlan1`)
- Sense HAT v1 (HTS221, LPS25H, LSM9DS1)
- Apollo MSR-2 mmWave sensor, flashed with ESPHome, attached via the Pi's own hotspot (`wlan0`)
- Jabra Speak 410 (USB; intercom service)

## Architecture

```
Location A                                                             Location B
┌────────────────────────────────────┐                                 ┌─────────────────────────┐
│ pi-viscous                         │                                 │ pi-wave                 │
│  wlan0 ─AP─ pi-mesh-viscous        │                                 │  wlan0 ─AP─ pi-mesh-wave│
│         └─── MSR-2 (192.168.10.10) │                                 │         └─── MSR-2      │
│  wlan1 ── home wifi ───────────────┼───────── 🌐 ── tailnet ─────────┼─ home wifi ── wlan1     │
│  Sense HAT (I²C)                   │                                 │  Sense HAT (I²C)        │
│                                    │                                 │                         │
│  Podman: nats-broker               │◀── nats-route over Tailscale ──▶│  nats-broker            │
│  systemd: sensehat,                │                                 │  systemd: sensehat,     │
│           mmwave                   │                                 │           mmwave        │
└────────────────────────────────────┘                                 └─────────────────────────┘
```

Each Pi runs its own NATS broker (Podman Quadlet). Brokers cluster over Tailscale MagicDNS. Per-service NKey auth for clients; shared token for cluster routes.

## Subject scheme

| Tree | Pattern | Use |
|---|---|---|
| `sensors.<host>.<device>.<metric>` | `sensors.pi-viscous.sensehat.temperature` | Live readings |
| `info.<host>.<device>.<metric>` | `info.pi-viscous.msr2.firmware_version` | Static / boot-time |
| `config.<host>.<device>.<setting>` | `config.pi-viscous.msr2.radar_timeout` | Tunable settings |
| `command.<host>.<device>.<action>` | (reserved) | Actuation, future |
| `events.<…>` | (reserved) | Logic-engine output, future |

Message envelope is JSON with `ts, host, device, metric, value, unit?, source?`. `value` is a scalar except for IMU readings (acceleration, gyroscope, magnetometer, orientation) where it's `{x, y, z}` or `{pitch, roll, yaw}`.

## Prerequisites

On the operator's machine:

- Ansible ≥ 2.15 with `community.general`, `ansible.posix` collections
  ```bash
  ansible-galaxy collection install community.general ansible.posix
  ```
- `nk` (NATS NKeys CLI): `go install github.com/nats-io/nkeys/nk@latest`
- `openssl` (for the cluster token)
- A Tailscale account with an auth key (reusable, ephemeral OK)

On each Pi (one-time, manual):

- Raspberry Pi OS Trixie (64-bit) flashed and reachable via the home network on `wlan1`
- A user with SSH + passwordless `sudo` (default: `pi`)
- The MSR-2 already flashed with ESPHome, configured to associate to the per-Pi hotspot SSID (`pi-mesh-<nickname>`) with the matching PSK. The ESPHome API runs unencrypted (no `api.encryption.key`) — the hotspot subnet only ever contains this Pi and this MSR-2, with no route to the internet

## One-time bootstrap

1. **Generate NKey credentials and the cluster token:**
   ```bash
   ./scripts/generate_nkeys.sh > /tmp/keys.txt
   ```
   The output contains two YAML blocks. Paste the first into `ansible/group_vars/all/vars.yml` (under `nats_users`), and the second into `ansible/group_vars/all/vault.yml`.

2. **Fill in the rest of `vault.yml`:**
   - `vault_tailscale_authkey`
   - `vault_upstream_wifi.<host>` (SSID + PSK of each Pi's home WiFi)
   - `vault_hotspot_psk.<host>`

3. **Encrypt the vault:**
   ```bash
   echo -n "$(openssl rand -base64 32)" > .vault_pass.txt
   chmod 600 .vault_pass.txt
   ansible-vault encrypt ansible/group_vars/all/vault.yml
   ```

4. **Configure inventory and per-host vars:**
   - Copy `ansible.cfg.example` → `ansible.cfg`
   - Copy `ansible/inventory/hosts.ini.example` → `ansible/inventory/hosts.ini` and fill in the Tailscale MagicDNS names
   - Fill `tailnet_domain` in `vars.yml`
   - Set the MSR-2 MAC in each `ansible/host_vars/<host>.yml`

5. **Run the playbook:**

   *First run on a new Pi* — the host isn't on the tailnet yet, so override
   `ansible_host` with its LAN address and limit the run to that one host:
   ```bash
   ansible-playbook ansible/site.yml -l viscous -e ansible_host=192.168.1.236
   ```

   *Steady-state* — once Tailscale is up, the MagicDNS name in the inventory
   resolves, so no overrides needed:
   ```bash
   ansible-playbook ansible/site.yml
   ```

## Verifying

From your laptop (with the `nats` CLI installed):

```bash
nats --server=nats://pi-viscous.tailnet-XXXX.ts.net:4222 \
     --nkey=/etc/nats/seeds/<some-seed> \
     sub 'sensors.>'
```

Or SSH to a Pi and inspect locally:

```bash
sudo systemctl status nats-broker sensehat mmwave
journalctl -u sensehat -f
```

## Operations

- **Rotate a service key**: regenerate one entry, replace in `vars.yml` (public) and `vault.yml` (seed), re-run Ansible.
- **Add a sensor metric**: extend `src/bare_metal/sensors/<file>.py`; re-run.
- **Add a new MSR-2 entity to the sensor tree**: add it to `msr2_entity_map.sensors` in `vars.yml`; re-run.
- **Engineering-mode gate energies**: flip `radar_engineering_mode` from any NATS client; the mmwave publisher will pick up the state change and start streaming `sensors.<host>.msr2.gates.*`.
