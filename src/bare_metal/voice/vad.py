"""WebRTC VAD wrapper for speech endpoint detection.

Uses the webrtcvad library (Google's WebRTC voice activity detector).
Stateless — no model files or LSTM hidden state required.

Each call to is_speech() splits the 80 ms chunk into four 20 ms frames
and applies a majority vote, so a chunk is considered speech if more than
half of its frames contain speech.

aggressiveness controls sensitivity:
  0 — least aggressive (accepts more speech, misses less)
  1 — moderate
  2 — moderate-aggressive  (good default)
  3 — most aggressive (fewest false positives, may clip quiet speech)
"""

from __future__ import annotations

import webrtcvad

_SAMPLE_RATE = 16_000
_FRAME_MS = 20                      # webrtcvad accepts 10, 20, or 30 ms
_FRAME_SAMPLES = _SAMPLE_RATE * _FRAME_MS // 1000   # 320 samples
_FRAME_BYTES = _FRAME_SAMPLES * 2   # 2 bytes per int16 sample


class WebRTCVAD:
    def __init__(self, aggressiveness: int = 2) -> None:
        self._vad = webrtcvad.Vad(aggressiveness)

    def reset(self) -> None:
        """No-op: webrtcvad is stateless."""

    def is_speech(self, chunk_int16: bytes) -> bool:
        """Return True if the majority of 20 ms frames in the chunk contain speech."""
        speech = 0
        total = 0
        for offset in range(0, len(chunk_int16) - _FRAME_BYTES + 1, _FRAME_BYTES):
            frame = chunk_int16[offset : offset + _FRAME_BYTES]
            try:
                if self._vad.is_speech(frame, _SAMPLE_RATE):
                    speech += 1
                total += 1
            except Exception:
                pass
        return total > 0 and speech > total // 2
