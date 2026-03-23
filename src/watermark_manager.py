"""
WatermarkManager: tracks event-time progress for stream processing.

Watermark formula:
    watermark = max(event_time seen so far) - watermark_delay_seconds

Events with event_time >= watermark are ON-TIME.
Events with event_time <  watermark are LATE → quarantine.

State persists across DAG runs via a JSON file to simulate a real streaming system.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class WatermarkManager:
    def __init__(self, watermark_delay_seconds: int, state_file: Path):
        self.watermark_delay_seconds = watermark_delay_seconds
        self.state_file = Path(state_file)
        self.max_event_time: datetime | None = None

    def load_state(self) -> None:
        """Read max_event_time from the JSON state file if it exists."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                data = json.load(f)
            ts = data.get("max_event_time")
            if ts:
                self.max_event_time = datetime.fromisoformat(ts)

    def save_state(self) -> None:
        """Persist max_event_time to the JSON state file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "max_event_time": (
                self.max_event_time.isoformat() if self.max_event_time else None
            ),
            "watermark_delay_seconds": self.watermark_delay_seconds,
        }
        with open(self.state_file, "w") as f:
            json.dump(payload, f, indent=2)

    def update(self, event_time: datetime) -> None:
        """Advance max_event_time if event_time is newer. Late events cannot lower the watermark."""
        if self.max_event_time is None or event_time > self.max_event_time:
            self.max_event_time = event_time

    def get_watermark(self) -> datetime | None:
        """Return current watermark (max_event_time - delay), or None before first event."""
        if self.max_event_time is None:
            return None
        from datetime import timedelta

        return self.max_event_time - timedelta(seconds=self.watermark_delay_seconds)

    def is_late(self, event_time: datetime) -> bool:
        """Return True if event_time falls behind the current watermark boundary."""
        watermark = self.get_watermark()
        if watermark is None:
            return False  # No watermark established yet → accept all events
        return event_time < watermark

    def describe(self) -> str:
        """Human-readable log string of current watermark state."""
        wm = self.get_watermark()
        max_et = self.max_event_time.isoformat() if self.max_event_time else "None"
        wm_str = wm.isoformat() if wm else "None"
        return (
            f"[WATERMARK STATE] max_event_time={max_et}  "
            f"watermark={wm_str}  delay={self.watermark_delay_seconds}s"
        )
