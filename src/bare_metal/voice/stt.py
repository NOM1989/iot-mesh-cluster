"""Vosk STT wrapper.

Loads the Vosk model once at startup.  Accepts a complete utterance
as raw 16-bit PCM (16 kHz mono) and returns the transcribed text.

A fresh KaldiRecognizer is created per utterance to avoid state
leakage between recordings.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os

from vosk import KaldiRecognizer, Model

log = logging.getLogger(__name__)

_SAMPLE_RATE = 16_000


@contextlib.contextmanager
def _quiet_stderr():
    """Suppress stderr output from the Vosk model loader."""
    devnull = os.open(os.devnull, os.O_WRONLY)
    old = os.dup(2)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(old, 2)
        os.close(old)


class VoskSTT:
    def __init__(self, model_path: str) -> None:
        log.info("Loading Vosk model from %s", model_path)
        with _quiet_stderr():
            self._model = Model(model_path)

    def transcribe(self, audio_buffer: bytes) -> str:
        """Transcribe a complete utterance; returns the recognised text or ''."""
        rec = KaldiRecognizer(self._model, _SAMPLE_RATE)
        rec.SetWords(False)
        rec.AcceptWaveform(audio_buffer)
        result = json.loads(rec.FinalResult())
        text = result.get("text", "").strip()
        log.info("STT result: %r", text)
        return text
