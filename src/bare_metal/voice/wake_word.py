"""openWakeWord wrapper.

Wraps a single openWakeWord TFLite model (hey_jarvis by default).
Operates on 16 kHz mono int16 PCM chunks.  Inference is synchronous
and fast enough (~3 ms on Pi 4) to call directly from the asyncio loop.
"""

from __future__ import annotations

import numpy as np
import openwakeword

_THRESHOLD = 0.5


class WakeWordDetector:
    def __init__(self, model_name: str = "hey_jarvis", threshold: float = _THRESHOLD) -> None:
        self._threshold = threshold
        # openwakeword.utils.download_models() must have been called first
        # (done at deploy time by the 05_voice_assistant Ansible role).
        # Use ONNX backend — tflite-runtime has no Python 3.13 ARM64 wheel.
        self._model = openwakeword.Model(
            wakeword_models=[model_name],
            inference_framework="onnx",
        )

    def reset(self) -> None:
        """Flush the model's audio context window.

        openWakeWord uses a ~1.5 s sliding spectrogram buffer.  After a
        detection, call this so the buffer no longer contains wake-word
        features when we return to LISTENING.  25 × 80 ms = 2 s of
        silence is enough to evict the entire window.
        """
        silence = np.zeros(1280, dtype=np.int16)
        for _ in range(25):
            self._model.predict(silence)

    def predict(self, chunk_int16: bytes) -> bool:
        """Return True if wake word score exceeds threshold on this chunk."""
        audio = np.frombuffer(chunk_int16, dtype=np.int16)
        predictions = self._model.predict(audio)
        return any(v >= self._threshold for v in predictions.values())
