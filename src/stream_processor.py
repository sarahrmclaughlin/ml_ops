"""
StreamProcessor: routes events as ON-TIME or LATE based on the current watermark.

Each event is logged with its verdict. A formatted summary box is printed at the end.
On-time events go to data/processed/<run_id>_on_time.ndjson.
Late events go to data/processed/<run_id>_quarantine.ndjson.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from watermark_manager import WatermarkManager

log = logging.getLogger("airflow.task")

DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", Path(__file__).parent.parent / "data"))


@dataclass
class ProcessingReport:
    run_id: str
    total: int
    on_time: list[dict] = field(default_factory=list)
    late: list[dict] = field(default_factory=list)
    final_watermark: datetime | None = None


class StreamProcessor:
    def __init__(self, watermark_manager: WatermarkManager, data_dir: Path | None = None):
        self.wm = watermark_manager
        self.data_dir = Path(data_dir) if data_dir else DATA_DIR

    def process_file(self, filepath: Path) -> ProcessingReport:
        """Read an ndjson file, route each event, and return a ProcessingReport."""
        filepath = Path(filepath)
        run_id = filepath.stem

        events = []
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))

        log.info(self.wm.describe())

        report = ProcessingReport(run_id=run_id, total=len(events))

        for event in events:
            event_time = datetime.fromisoformat(event["event_time"])
            self.wm.update(event_time)

            if self.wm.is_late(event_time):
                wm = self.wm.get_watermark()
                seconds_late = int((wm - event_time).total_seconds()) if wm else 0
                log.info(
                    "[EVENT] %-12s  event_time=%s  feature1=%-8s  → LATE (%ds behind watermark)"
                    " *** QUARANTINED ***",
                    event["event_id"],
                    event["event_time"],
                    event["feature1"],
                    seconds_late,
                )
                report.late.append(event)
            else:
                log.info(
                    "[EVENT] %-12s  event_time=%s  feature1=%-8s  → ON-TIME",
                    event["event_id"],
                    event["event_time"],
                    event["feature1"],
                )
                report.on_time.append(event)

        report.final_watermark = self.wm.get_watermark()
        self._log_summary(report)
        return report

    def write_on_time(self, events: list[dict], run_id: str) -> Path:
        return self._write_ndjson(events, f"{run_id}_on_time.ndjson")

    def write_quarantine(self, events: list[dict], run_id: str) -> Path:
        return self._write_ndjson(events, f"{run_id}_quarantine.ndjson")

    def _write_ndjson(self, events: list[dict], filename: str) -> Path:
        out_dir = self.data_dir / "processed"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename
        with open(out_path, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        return out_path

    def _log_summary(self, report: ProcessingReport) -> None:
        wm_str = report.final_watermark.isoformat() if report.final_watermark else "None"
        late_detail = ""
        if report.late:
            first_late = report.late[0]
            wm = report.final_watermark
            if wm:
                late_et = datetime.fromisoformat(first_late["event_time"])
                seconds_late = int((wm - late_et).total_seconds())
                late_detail = f"Late event: feature1={first_late['feature1']}, {seconds_late}s late"
            else:
                late_detail = f"Late event: feature1={first_late['feature1']}"

        lines = [
            "WATERMARK DEMO — SUMMARY",
            f"Total: {report.total}  |  On-time: {len(report.on_time)}  |  Late: {len(report.late)}",
            f"Watermark boundary: {wm_str}",
        ]
        if late_detail:
            lines.append(late_detail)

        width = max(len(l) for l in lines) + 4
        border = "═" * width
        log.info("╔%s╗", border)
        for line in lines:
            log.info("║  %-*s  ║", width - 4, line)
        log.info("╚%s╝", border)
