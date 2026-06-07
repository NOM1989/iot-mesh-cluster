"""Silero VAD wrapper using onnxruntime — no PyTorch dependency.

Loads the ONNX export of Silero VAD v4. Maintains LSTM hidden state
across chunks so context is preserved within a single utterance.
Call reset() before each new recording window.

ONNX model inputs (v4 canonical export):
  input  float32 [1, num_samples]   normalised audio at 16 kHz
  h      float32 [2, 1, 64]         LSTM hidden state
  c      float32 [2, 1, 64]         LSTM cell state
  sr     int64   scalar             sample rate (16000)
"""

from __future__ import annotations

import numpy as np
import onnxruntime as ort

_SAMPLE_RATE = np.array(16_000, dtype=np.int64)


class SileroVAD:
    def __init__(self, model_path: str, threshold: float = 0.5) -> None:
        opts = ort.SessionOptions()
        opts.inter_op_num_threads = 1
        opts.intra_op_num_threads = 1
        self._session = ort.InferenceSession(model_path, sess_options=opts)
        self._threshold = threshold
        self.reset()

    def reset(self) -> None:
        """Reset LSTM hidden state — call at the start of each recording window."""
        self._h = np.zeros((2, 1, 64), dtype=np.float32)
        self._c = np.zeros((2, 1, 64), dtype=np.float32)

    def is_speech(self, chunk_int16: bytes) -> bool:
        """Return True if the chunk likely contains speech."""
        audio = np.frombuffer(chunk_int16, dtype=np.int16).astype(np.float32) / 32768.0
        audio = audio[np.newaxis, :]  # [1, num_samples]
        outs = self._session.run(
            None,
            {
                "input": audio,
                "h": self._h,
                "c": self._c,
                "sr": _SAMPLE_RATE,
            },
        )
        # outs[0] = speech probability [1, 1], outs[1] = new h, outs[2] = new c
        prob = float(outs[0].squeeze())
        self._h = outs[1]
        self._c = outs[2]
        return prob >= self._threshold
