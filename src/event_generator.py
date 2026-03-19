"""
EventGenerator: produces synthetic ML feature events for the watermarking demo.

Normal events:  feature1 ~ Gaussian(100, 5), event_times spaced evenly.
Late event:     feature1=10000 (intentionally bad), event_time far in the past.

Output format: newline-delimited JSON (ndjson) written to data/raw_events/.
"""

from __future__ import annotations

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path(__file__).parent.parent / "data"))


class EventGenerator:
    def __init__(self, data_dir: Path | None = None, seed: int | None = None):
        self.data_dir = Path(data_dir) if data_dir else DATA_DIR
        self._rng = random.Random(seed)

    def generate_normal_batch(
        self,
        count: int,
        base_time: datetime,
        interval_seconds: int = 5,
    ) -> list[dict]:
        """Generate `count` normal events with feature1 ~ Gauss(100, 5)."""
        events = []
        for i in range(count):
            event_time = base_time + timedelta(seconds=i * interval_seconds)
            events.append(
                {
                    "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                    "event_time": event_time.isoformat(),
                    "feature1": round(self._rng.gauss(100, 5), 2),
                }
            )
        return events

    def generate_late_event(
        self,
        late_by_seconds: int,
        current_max_event_time: datetime,
    ) -> dict:
        """Generate a deliberately bad event that falls behind the watermark."""
        event_time = current_max_event_time - timedelta(seconds=late_by_seconds)
        return {
            "event_id": f"evt_{uuid.uuid4().hex[:8]}",
            "event_time": event_time.isoformat(),
            "feature1": 10000,  # anomalous value
        }

    def write_batch(self, events: list[dict]) -> Path:
        """Write events as ndjson to data/raw_events/<timestamp>_events.ndjson."""
        out_dir = self.data_dir / "raw_events"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        out_path = out_dir / f"{ts}_events.ndjson"
        with open(out_path, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        return out_path
