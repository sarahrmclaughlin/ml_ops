"""
watermark_demo DAG — MLOps watermarking demo

Demonstrates stream-processing watermarks (Flink/Spark style) in an Airflow pipeline.

Trigger with params:
  {"run_mode": "normal"}       → Run 1: 10 normal events, all ON-TIME
  {"run_mode": "inject_late"}  → Run 2: 3 normal + 1 deliberately bad event (QUARANTINED)

Watermark concept:
    watermark = max(event_time seen) - 30s
    event_time >= watermark  →  ON-TIME
    event_time <  watermark  →  LATE → quarantine
"""

import sys
sys.path.insert(0, "/opt/airflow/src")

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
WATERMARK_STATE_FILE = DATA_DIR / "watermarks" / "watermark_state.json"
WATERMARK_DELAY_SECONDS = 30


default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="watermark_demo",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"run_mode": "normal"},
    tags=["mlops", "watermarking", "demo"],
) as dag:

    # ─────────────────────────────────────────────
    # Task 1: Generate events and write to ndjson
    # ─────────────────────────────────────────────
    def generate_events(**context):
        from event_generator import EventGenerator

        run_mode = context["params"]["run_mode"]
        base_time = datetime.now(tz=timezone.utc).replace(microsecond=0)

        gen = EventGenerator(data_dir=DATA_DIR)

        if run_mode == "normal":
            log.info("Run mode: NORMAL — generating 10 events")
            events = gen.generate_normal_batch(count=10, base_time=base_time, interval_seconds=5)

        elif run_mode == "inject_late":
            log.info("Run mode: INJECT_LATE — generating 3 normal + 1 late event")
            events = gen.generate_normal_batch(count=3, base_time=base_time, interval_seconds=5)

            # Determine current max event time from the normal batch
            current_max = datetime.fromisoformat(events[-1]["event_time"])

            # Late event is 60s behind current_max (well behind the watermark boundary)
            late_event = gen.generate_late_event(
                late_by_seconds=60,
                current_max_event_time=current_max,
            )
            events.append(late_event)
            log.info(
                "Injected late event: event_id=%s  event_time=%s  feature1=%s",
                late_event["event_id"],
                late_event["event_time"],
                late_event["feature1"],
            )
        else:
            raise ValueError(f"Unknown run_mode: {run_mode!r}. Use 'normal' or 'inject_late'.")

        out_path = gen.write_batch(events)
        log.info("Wrote %d events to %s", len(events), out_path)

        # Push the file path via XCom so the next task can read it
        context["ti"].xcom_push(key="event_file", value=str(out_path))

    task_generate_events = PythonOperator(
        task_id="task_generate_events",
        python_callable=generate_events,
    )

    # ─────────────────────────────────────────────
    # Task 2: Load watermark, process events, route
    # ─────────────────────────────────────────────
    def process_stream(**context):
        from watermark_manager import WatermarkManager
        from stream_processor import StreamProcessor

        event_file = context["ti"].xcom_pull(key="event_file", task_ids="task_generate_events")
        if not event_file:
            raise RuntimeError("No event file received from task_generate_events via XCom.")

        wm = WatermarkManager(
            watermark_delay_seconds=WATERMARK_DELAY_SECONDS,
            state_file=WATERMARK_STATE_FILE,
        )
        wm.load_state()
        log.info("Loaded watermark state: %s", wm.describe())

        processor = StreamProcessor(watermark_manager=wm, data_dir=DATA_DIR)
        report = processor.process_file(Path(event_file))

        # Persist on-time and quarantine files
        on_time_path = processor.write_on_time(report.on_time, run_id=report.run_id)
        quarantine_path = processor.write_quarantine(report.late, run_id=report.run_id)

        log.info("On-time events written to: %s", on_time_path)
        log.info("Quarantine events written to: %s", quarantine_path)

        # Persist updated watermark state
        wm.save_state()
        log.info("Watermark state saved: %s", wm.describe())

        # Push report summary via XCom
        summary = {
            "run_id": report.run_id,
            "total": report.total,
            "on_time_count": len(report.on_time),
            "late_count": len(report.late),
            "final_watermark": report.final_watermark.isoformat() if report.final_watermark else None,
            "late_events": report.late,
            "on_time_path": str(on_time_path),
            "quarantine_path": str(quarantine_path),
        }
        context["ti"].xcom_push(key="processing_summary", value=summary)

    task_process_stream = PythonOperator(
        task_id="task_process_stream",
        python_callable=process_stream,
    )

    # ─────────────────────────────────────────────
    # Task 3: Print a formatted summary to logs
    # ─────────────────────────────────────────────
    def print_summary(**context):
        summary = context["ti"].xcom_pull(
            key="processing_summary", task_ids="task_process_stream"
        )
        if not summary:
            log.warning("No summary received from task_process_stream.")
            return

        wm_str = summary["final_watermark"] or "None"
        late_detail = ""
        if summary["late_events"]:
            first_late = summary["late_events"][0]
            if summary["final_watermark"]:
                wm = datetime.fromisoformat(summary["final_watermark"])
                late_et = datetime.fromisoformat(first_late["event_time"])
                seconds_late = int((wm - late_et).total_seconds())
                late_detail = f"Late event: feature1={first_late['feature1']}, {seconds_late}s late"
            else:
                late_detail = f"Late event: feature1={first_late['feature1']}"

        lines = [
            "WATERMARK DEMO — SUMMARY",
            f"Total: {summary['total']}  |  On-time: {summary['on_time_count']}  |  Late: {summary['late_count']}",
            f"Watermark boundary: {wm_str}",
        ]
        if late_detail:
            lines.append(late_detail)
        lines.append(f"On-time file:    {summary['on_time_path']}")
        lines.append(f"Quarantine file: {summary['quarantine_path']}")

        width = max(len(l) for l in lines) + 4
        border = "═" * width
        log.info("╔%s╗", border)
        for line in lines:
            log.info("║  %-*s  ║", width - 4, line)
        log.info("╚%s╝", border)

    task_print_summary = PythonOperator(
        task_id="task_print_summary",
        python_callable=print_summary,
    )

    # DAG wiring
    task_generate_events >> task_process_stream >> task_print_summary
