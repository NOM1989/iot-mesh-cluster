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

    def predict(self, chunk_int16: bytes) -> bool:
        """Return True if wake word score exceeds threshold on this chunk."""
        audio = np.frombuffer(chunk_int16, dtype=np.int16)
        predictions = self._model.predict(audio)
        return any(v >= self._threshold for v in predictions.values())
