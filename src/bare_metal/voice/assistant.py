"""Voice assistant: wake word → VAD → STT → NATS publish.

Audio pipeline:
  PyAudio (16 kHz mono) → openWakeWord → Silero VAD (ONNX) → Vosk → NATS

State machine:
  LISTENING  — openWakeWord watches the mic stream chunk-by-chunk
  TRIGGER    — wake word detected; chime + blue LED pulse triggered
  RECORDING  — Silero VAD accumulates audio until silence
  PROCESSING — Vosk transcribes the buffered audio (chunks are discarded)
  SUSPENDED  — intercom call active; PyAudio stream is closed

Transcript output subject: sensors.{host}.voice.transcript
  Payload: {"ts": "<iso>", "host": "<label>", "text": "<utterance>"}

Config (from /etc/iot-mesh/config.json, section "voice"):
  audio_device    ALSA device name          (default: plughw:USB)
  vosk_model      path to Vosk model dir    (default: .../vosk-model-small-en-us-0.15)
  silero_model    path to .onnx file        (default: .../silero_vad.onnx)
  wake_model      openWakeWord model name   (default: hey_jarvis)
  vad_threshold   speech probability cutoff (default: 0.5)
  vad_silence_ms  silence → end recording   (default: 800)
  chunk_ms        audio chunk size in ms    (default: 80)
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

import pyaudio
import nats
from nats.aio.msg import Msg

from bare_metal.common import load_runtime_config, utc_now_iso

from .wake_word import WakeWordDetector
from .vad import SileroVAD
from .stt import VoskSTT

log = logging.getLogger(__name__)

_SOUNDS_DIR = Path(__file__).parent.parent / "media" / "sounds"
_SAMPLE_RATE = 16_000
_FORMAT = pyaudio.paInt16
_CHANNELS = 1


class State(Enum):
    LISTENING = auto()
    TRIGGER = auto()
    RECORDING = auto()
    PROCESSING = auto()
    SUSPENDED = auto()


class VoiceAssistant:
    def __init__(
        self,
        nc: nats.NATS,
        host_label: str,
        device_index: int,
        chunk_frames: int,
        vad_silence_ms: int,
        wake: WakeWordDetector,
        vad: SileroVAD,
        stt: VoskSTT,
    ) -> None:
        self._nc = nc
        self._host = host_label
        self._device_index = device_index
        self._chunk_frames = chunk_frames
        self._silence_chunks = max(1, vad_silence_ms * _SAMPLE_RATE // (chunk_frames * 1000))
        self._wake = wake
        self._vad = vad
        self._stt = stt
        self._state = State.LISTENING
        self._pa: pyaudio.PyAudio | None = None
        self._stream: Any | None = None

    # ── LED control ───────────────────────────────────────────────────

    async def _led_pulse_blue(self) -> None:
        await self._nc.publish(
            f"command.{self._host}.sensehat.matrix",
            json.dumps(
                {"effect": "pulse", "color": [0, 0, 255], "speed": 0.5},
                separators=(",", ":"),
            ).encode(),
        )

    async def _led_off(self) -> None:
        await self._nc.publish(
            f"command.{self._host}.sensehat.matrix",
            json.dumps({"effect": "off"}, separators=(",", ":")).encode(),
        )

    # ── Sound ─────────────────────────────────────────────────────────

    def _play_wake_chime_bg(self, device: str) -> None:
        subprocess.Popen(
            ["aplay", "-q", "-D", device, str(_SOUNDS_DIR / "wake.wav")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    # ── PyAudio stream ────────────────────────────────────────────────

    def _open_stream(self) -> None:
        if self._pa is None:
            self._pa = pyaudio.PyAudio()
        self._stream = self._pa.open(
            format=_FORMAT,
            channels=_CHANNELS,
            rate=_SAMPLE_RATE,
            input=True,
            input_device_index=self._device_index,
            frames_per_buffer=self._chunk_frames,
        )
        log.info("PyAudio stream opened (device_index=%d chunk_frames=%d)",
                 self._device_index, self._chunk_frames)

    def _close_stream(self) -> None:
        if self._stream is not None:
            self._stream.stop_stream()
            self._stream.close()
            self._stream = None
        log.info("PyAudio stream closed")

    def _read_chunk(self) -> bytes | None:
        if self._stream is None:
            return None
        try:
            return self._stream.read(self._chunk_frames, exception_on_overflow=False)
        except OSError as exc:
            log.warning("PyAudio read error: %s", exc)
            return None

    # ── Intercom conflict monitor ─────────────────────────────────────

    async def on_intercom_msg(self, msg: Msg) -> None:
        try:
            body = json.loads(msg.data)
        except ValueError:
            return
        action = body.get("action", "")
        if action in ("call_out", "call_request", "call_accept", "call_accepted"):
            if self._state != State.SUSPENDED:
                log.info("Intercom active — suspending voice assistant")
                self._close_stream()
                self._state = State.SUSPENDED
        elif action in ("call_terminate", "call_terminated", "call_declined", "call_decline"):
            if self._state == State.SUSPENDED:
                log.info("Intercom idle — resuming voice assistant")
                self._open_stream()
                self._vad.reset()
                self._state = State.LISTENING

    # ── Transcript publish ────────────────────────────────────────────

    async def _publish_transcript(self, text: str) -> None:
        payload = json.dumps(
            {"ts": utc_now_iso(), "host": self._host, "text": text},
            separators=(",", ":"),
        ).encode()
        await self._nc.publish(f"sensors.{self._host}.voice.transcript", payload)
        log.info("Transcript published: %r", text)

    # ── Main pipeline ─────────────────────────────────────────────────

    async def run(
        self,
        loop: asyncio.AbstractEventLoop,
        stop: asyncio.Event,
        audio_device: str,
    ) -> None:
        self._open_stream()
        try:
            await self._pipeline_loop(loop, stop, audio_device)
        finally:
            self._close_stream()
            if self._pa is not None:
                self._pa.terminate()
                self._pa = None

    async def _pipeline_loop(
        self,
        loop: asyncio.AbstractEventLoop,
        stop: asyncio.Event,
        audio_device: str,
    ) -> None:
        audio_buffer: list[bytes] = []
        consecutive_silence = 0

        while not stop.is_set():
            if self._state == State.SUSPENDED:
                await asyncio.sleep(0.1)
                continue

            chunk = await loop.run_in_executor(None, self._read_chunk)
            if chunk is None:
                await asyncio.sleep(0.05)
                continue

            if self._state == State.LISTENING:
                if self._wake.predict(chunk):
                    log.info("Wake word detected")
                    self._state = State.TRIGGER
                    self._play_wake_chime_bg(audio_device)
                    await self._led_pulse_blue()
                    self._vad.reset()
                    audio_buffer = []
                    consecutive_silence = 0
                    # Guard against intercom suspension during the LED await
                    if self._state == State.TRIGGER:
                        self._state = State.RECORDING

            elif self._state == State.RECORDING:
                audio_buffer.append(chunk)
                is_speech = await loop.run_in_executor(None, self._vad.is_speech, chunk)
                if is_speech:
                    consecutive_silence = 0
                else:
                    consecutive_silence += 1

                if consecutive_silence >= self._silence_chunks:
                    log.info("Silence detected — transcribing %d chunks", len(audio_buffer))
                    self._state = State.PROCESSING
                    full_audio = b"".join(audio_buffer)
                    audio_buffer = []
                    consecutive_silence = 0

                    text = await loop.run_in_executor(None, self._stt.transcribe, full_audio)

                    await self._led_off()
                    if text:
                        await self._publish_transcript(text)
                    # Preserve SUSPENDED state if intercom interrupted during STT
                    if self._state == State.PROCESSING:
                        self._state = State.LISTENING


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_runtime_config("VOICE_SEED")
    voice_cfg: dict[str, Any] = cfg.section("voice") or {}

    audio_device: str = voice_cfg.get("audio_device", "plughw:USB")
    vosk_model_path: str = voice_cfg.get(
        "vosk_model",
        "/opt/iot-mesh-cluster/models/voice/vosk-model-small-en-us-0.15",
    )
    silero_model_path: str = voice_cfg.get(
        "silero_model",
        "/opt/iot-mesh-cluster/models/voice/silero_vad.onnx",
    )
    wake_model_name: str = voice_cfg.get("wake_model", "hey_jarvis")
    vad_threshold: float = float(voice_cfg.get("vad_threshold", 0.5))
    vad_silence_ms: int = int(voice_cfg.get("vad_silence_ms", 800))
    chunk_ms: int = int(voice_cfg.get("chunk_ms", 80))
    chunk_frames: int = _SAMPLE_RATE * chunk_ms // 1000

    # Probe PyAudio for the USB audio input device index
    pa_probe = pyaudio.PyAudio()
    device_index = 0
    for i in range(pa_probe.get_device_count()):
        info = pa_probe.get_device_info_by_index(i)
        if int(info["maxInputChannels"]) > 0 and "USB" in str(info["name"]):
            device_index = i
            log.info("Using PyAudio device %d: %s", i, info["name"])
            break
    pa_probe.terminate()

    log.info("Loading Silero VAD from %s", silero_model_path)
    vad = SileroVAD(silero_model_path, threshold=vad_threshold)
    log.info("Loading wake word model: %s", wake_model_name)
    wake = WakeWordDetector(model_name=wake_model_name)

    # Vosk logs its own "Loading model" line
    stt = VoskSTT(vosk_model_path)

    async def _on_error(e: Exception) -> None:
        log.error("NATS error: %s", e)

    async def _on_disconnected() -> None:
        log.warning("NATS disconnected")

    async def _on_reconnected() -> None:
        log.info("NATS reconnected")

    nc = await nats.connect(
        servers=[cfg.nats_url],
        nkeys_seed=cfg.seed_path,
        name=f"voice@{cfg.host_label}",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
        ping_interval=20,
        error_cb=_on_error,
        disconnected_cb=_on_disconnected,
        reconnected_cb=_on_reconnected,
    )

    assistant = VoiceAssistant(
        nc=nc,
        host_label=cfg.host_label,
        device_index=device_index,
        chunk_frames=chunk_frames,
        vad_silence_ms=vad_silence_ms,
        wake=wake,
        vad=vad,
        stt=stt,
    )

    intercom_sub = await nc.subscribe(
        f"command.{cfg.host_label}.intercom.*",
        cb=assistant.on_intercom_msg,
    )
    log.info("Voice assistant started (host=%s wake=%s)", cfg.host_label, wake_model_name)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await assistant.run(loop, stop, audio_device)
    finally:
        await intercom_sub.unsubscribe()
        await nc.drain()
        log.info("voice-assistant stopped")


if __name__ == "__main__":
    asyncio.run(main())
