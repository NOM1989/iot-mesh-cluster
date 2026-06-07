"""Intercom: 2-way audio calling between mesh nodes.

Signalling flows through NATS on command.{host}.intercom.call.  Audio is
carried by GStreamer RTP/Opus pipelines launched as subprocesses.

Call flow (initiated by publishing to this Pi's command subject):

  call_out      → Pi publishes call_request to peer, enters RINGING_OUT
  call_request  ← received from peer, enter RINGING_IN
  call_accept   → Pi publishes call_accepted to peer, enters ACTIVE + starts audio
  call_accepted ← received from peer while RINGING_OUT, enters ACTIVE + starts audio
  call_decline  → Pi publishes call_declined to peer, returns to IDLE
  call_declined ← received from peer while RINGING_OUT, returns to IDLE
  call_terminate → Pi publishes call_terminated to peer, stops audio, returns to IDLE
  call_terminated ← received from peer, stops audio, returns to IDLE

Either Pi can send call_terminate at any time to end an active call.
Busy policy: an incoming call_request while RINGING_IN or ACTIVE is auto-declined.

Sounds:
  ring.wav      — gentle double-beep, loops while ringing on both ends
  connect.wav   — ba-dup, played once when call is accepted (both ends)
  disconnect.wav — played once when call ends (both ends)

Config (from /etc/iot-mesh/config.json):
  peer_tailscale_ips:         list of peer Tailscale IPs (first entry used)
  intercom.audio_device:      ALSA device name (default plughw:USB)
  intercom.audio_port:        UDP port for RTP audio (default 5000)
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import subprocess
from enum import Enum, auto
from pathlib import Path
from typing import Any

import nats

from bare_metal.common import load_runtime_config, utc_now_iso

log = logging.getLogger(__name__)

_CALL_SUBJECT = "command.{host}.intercom.call"

_SOUNDS_DIR = Path(__file__).parent / "sounds"


class State(Enum):
    IDLE = auto()
    RINGING_OUT = auto()
    RINGING_IN = auto()
    ACTIVE = auto()


def _ts_msg(**kwargs: Any) -> bytes:
    return json.dumps({"ts": utc_now_iso(), **kwargs}, separators=(",", ":")).encode()


def _build_gst_tx(device: str, peer_ip: str, port: int) -> list[str]:
    return [
        "gst-launch-1.0", "-q",
        "alsasrc", f"device={device}",
        "!", "audioconvert",
        "!", "audioresample",
        "!", "opusenc", "bitrate=32000",
        "!", "rtpopuspay",
        "!", "udpsink", f"host={peer_ip}", f"port={port}",
    ]


def _build_gst_rx(device: str, port: int) -> list[str]:
    return [
        "gst-launch-1.0", "-q",
        "udpsrc", f"port={port}",
        "!", "application/x-rtp,media=audio,encoding-name=OPUS,payload=96",
        "!", "rtpopusdepay",
        "!", "opusdec",
        "!", "audioconvert",
        "!", "alsasink", f"device={device}",
    ]


class Intercom:
    def __init__(
        self,
        nc: nats.NATS,
        local_host: str,
        peer_ip: str,
        audio_device: str,
        audio_port: int,
    ) -> None:
        self._nc = nc
        self._local_host = local_host
        self._peer_ip = peer_ip
        self._audio_device = audio_device
        self._audio_port = audio_port
        self._state = State.IDLE
        self._peer_host: str | None = None
        self._tx_proc: subprocess.Popen | None = None
        self._rx_proc: subprocess.Popen | None = None
        self._ring_task: asyncio.Task | None = None

    # ── NATS helpers ──────────────────────────────────────────────────

    def _local_subject(self) -> str:
        return _CALL_SUBJECT.format(host=self._local_host)

    def _peer_subject(self, peer: str | None = None) -> str:
        host = peer or self._peer_host
        if not host:
            raise RuntimeError("peer host unknown")
        return _CALL_SUBJECT.format(host=host)

    async def _publish(self, subject: str, **kwargs: Any) -> None:
        await self._nc.publish(subject, _ts_msg(**kwargs))

    # ── Sound helpers ─────────────────────────────────────────────────

    def _start_ring(self) -> None:
        # Cancel any existing ring task without waiting — safe because a new
        # loop iteration starts fresh and the old aplay exits on its own.
        if self._ring_task and not self._ring_task.done():
            self._ring_task.cancel()
        self._ring_task = asyncio.ensure_future(self._ring_loop())

    async def _stop_ring(self) -> None:
        """Cancel ring loop and wait for the aplay subprocess to fully exit
        before returning, so the ALSA device is free for the next sound."""
        task = self._ring_task
        self._ring_task = None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _ring_loop(self) -> None:
        path = str(_SOUNDS_DIR / "ring.wav")
        proc: asyncio.subprocess.Process | None = None
        try:
            while True:
                proc = await asyncio.create_subprocess_exec(
                    "aplay", "-q", "-D", self._audio_device, path,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
                proc = None
        except asyncio.CancelledError:
            if proc is not None and proc.returncode is None:
                proc.terminate()
                await proc.wait()
            raise

    async def _play_sound(self, name: str) -> None:
        """Play a sound file and await completion before returning."""
        path = str(_SOUNDS_DIR / name)
        try:
            proc = await asyncio.create_subprocess_exec(
                "aplay", "-q", "-D", self._audio_device, path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
        except Exception as exc:
            log.debug("Sound playback failed (%s): %s", name, exc)

    def _play_sound_bg(self, name: str) -> None:
        """Fire-and-forget sound playback (device must already be free)."""
        path = str(_SOUNDS_DIR / name)
        subprocess.Popen(
            ["aplay", "-q", "-D", self._audio_device, path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    # ── Audio helpers ─────────────────────────────────────────────────

    def _start_audio(self) -> None:
        log.info("Starting GStreamer audio (device=%s port=%d peer=%s)",
                 self._audio_device, self._audio_port, self._peer_ip)
        self._tx_proc = subprocess.Popen(
            _build_gst_tx(self._audio_device, self._peer_ip, self._audio_port),
        )
        self._rx_proc = subprocess.Popen(
            _build_gst_rx(self._audio_device, self._audio_port),
        )

    def _stop_audio(self) -> None:
        for proc in (self._tx_proc, self._rx_proc):
            if proc is not None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self._tx_proc = None
        self._rx_proc = None
        log.info("GStreamer audio stopped")

    # ── NATS message handler ──────────────────────────────────────────

    async def handle(self, msg: nats.aio.msg.Msg) -> None:
        try:
            body = json.loads(msg.data)
        except ValueError:
            log.warning("Unparseable intercom message on %s", msg.subject)
            return

        action: str = body.get("action", "")
        sender: str = body.get("from", "")

        log.info("state=%s action=%s from=%s", self._state.name, action, sender or "(local)")

        if action == "call_out":
            await self._handle_call_out(body)
        elif action == "call_request":
            await self._handle_call_request(sender)
        elif action == "call_accept":
            await self._handle_call_accept()
        elif action == "call_decline":
            await self._handle_call_decline()
        elif action == "call_accepted":
            await self._handle_call_accepted(sender)
        elif action == "call_declined":
            await self._handle_call_declined(sender)
        elif action == "call_terminate":
            await self._handle_terminate(sender)
        elif action == "call_terminated":
            await self._handle_terminated(sender)
        else:
            log.warning("Unknown intercom action: %s", action)

    # ── State machine ─────────────────────────────────────────────────

    async def _handle_call_out(self, body: dict) -> None:
        if self._state != State.IDLE:
            log.warning("call_out ignored — already %s", self._state.name)
            return
        to: str = body.get("to", "")
        if not to:
            log.warning("call_out missing 'to' field")
            return
        self._peer_host = to
        self._state = State.RINGING_OUT
        log.info("Calling %s ...", to)
        await self._publish(
            self._peer_subject(),
            action="call_request",
            **{"from": self._local_host},
        )
        self._start_ring()

    async def _handle_call_request(self, sender: str) -> None:
        if self._state in (State.RINGING_IN, State.ACTIVE):
            log.info("Busy — declining call from %s", sender)
            await self._publish(
                _CALL_SUBJECT.format(host=sender),
                action="call_declined",
                **{"from": self._local_host},
            )
            return
        if self._state == State.RINGING_OUT:
            # Simultaneous call — treat as peer answering.
            log.info("Simultaneous call with %s — treating as accepted", sender)
            self._peer_host = sender
            await self._stop_ring()
            await self._play_sound("connect.wav")
            self._state = State.ACTIVE
            self._start_audio()
            await self._publish(
                self._peer_subject(),
                action="call_accepted",
                **{"from": self._local_host},
            )
            return
        self._peer_host = sender
        self._state = State.RINGING_IN
        self._start_ring()
        log.info("Incoming call from %s — publish call_accept to answer", sender)

    async def _handle_call_accept(self) -> None:
        if self._state != State.RINGING_IN:
            log.warning("call_accept ignored — state is %s", self._state.name)
            return
        await self._stop_ring()
        await self._play_sound("connect.wav")
        self._state = State.ACTIVE
        self._start_audio()
        await self._publish(
            self._peer_subject(),
            action="call_accepted",
            **{"from": self._local_host},
        )
        log.info("Call accepted — audio active")

    async def _handle_call_decline(self) -> None:
        if self._state != State.RINGING_IN:
            log.warning("call_decline ignored — state is %s", self._state.name)
            return
        peer = self._peer_host
        await self._stop_ring()
        self._peer_host = None
        self._state = State.IDLE
        await self._publish(
            _CALL_SUBJECT.format(host=peer),
            action="call_declined",
            **{"from": self._local_host},
        )
        log.info("Call declined")

    async def _handle_call_accepted(self, sender: str) -> None:
        if self._state != State.RINGING_OUT:
            log.warning("call_accepted ignored — state is %s", self._state.name)
            return
        await self._stop_ring()
        await self._play_sound("connect.wav")
        self._state = State.ACTIVE
        self._start_audio()
        log.info("Call accepted by %s — audio active", sender)

    async def _handle_call_declined(self, sender: str) -> None:
        if self._state != State.RINGING_OUT:
            log.warning("call_declined ignored — state is %s", self._state.name)
            return
        await self._stop_ring()
        self._peer_host = None
        self._state = State.IDLE
        log.info("Call declined by %s", sender)

    async def _handle_terminate(self, sender: str) -> None:
        if self._state == State.IDLE:
            return
        peer = self._peer_host
        await self._stop_ring()
        self._state = State.IDLE
        self._peer_host = None
        if self._tx_proc is not None:
            self._stop_audio()
            self._play_sound_bg("disconnect.wav")
        if peer:
            await self._publish(
                _CALL_SUBJECT.format(host=peer),
                action="call_terminated",
                **{"from": self._local_host},
            )
        log.info("Call terminated (initiated by %s)", sender or "local")

    async def _handle_terminated(self, sender: str) -> None:
        if self._state == State.IDLE:
            return
        await self._stop_ring()
        self._state = State.IDLE
        self._peer_host = None
        if self._tx_proc is not None:
            self._stop_audio()
            self._play_sound_bg("disconnect.wav")
        log.info("Call ended by remote (%s)", sender)

    def cleanup(self) -> None:
        if self._ring_task and not self._ring_task.done():
            self._ring_task.cancel()
        self._ring_task = None
        if self._tx_proc is not None:
            self._stop_audio()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_runtime_config("INTERCOM_SEED")

    peer_ips: list[str] = cfg.section("peer_tailscale_ips") or []
    if not peer_ips:
        raise RuntimeError("peer_tailscale_ips is missing or empty in config.json")

    intercom_cfg: dict[str, Any] = cfg.section("intercom") or {}
    audio_device: str = intercom_cfg.get("audio_device", "plughw:USB")
    audio_port: int = int(intercom_cfg.get("audio_port", 5000))

    log.info(
        "Starting intercom: host=%s peer_ip=%s device=%s port=%d",
        cfg.host_label, peer_ips[0], audio_device, audio_port,
    )

    async def _on_error(e: Exception) -> None:
        log.error("NATS error: %s", e)

    async def _on_disconnected() -> None:
        log.warning("NATS disconnected")

    async def _on_reconnected() -> None:
        log.info("NATS reconnected")

    nc = await nats.connect(
        servers=[cfg.nats_url],
        nkeys_seed=cfg.seed_path,
        name=f"intercom@{cfg.host_label}",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
        ping_interval=20,
        error_cb=_on_error,
        disconnected_cb=_on_disconnected,
        reconnected_cb=_on_reconnected,
    )

    intercom = Intercom(
        nc=nc,
        local_host=cfg.host_label,
        peer_ip=peer_ips[0],
        audio_device=audio_device,
        audio_port=audio_port,
    )

    subject = _CALL_SUBJECT.format(host=cfg.host_label)
    sub = await nc.subscribe(subject, cb=intercom.handle)
    log.info("Subscribed to %s", subject)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    intercom.cleanup()
    await sub.unsubscribe()
    await nc.drain()
    log.info("intercom stopped")


if __name__ == "__main__":
    asyncio.run(main())
