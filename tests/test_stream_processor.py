import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from watermark_manager import WatermarkManager
from stream_processor import StreamProcessor, ProcessingReport


T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
DELAY = 30


@pytest.fixture
def state_file(tmp_path):
    return tmp_path / "watermarks" / "state.json"


@pytest.fixture
def wm(state_file):
    return WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)


@pytest.fixture
def processor(tmp_path, wm):
    return StreamProcessor(watermark_manager=wm, data_dir=tmp_path)


def write_ndjson(path: Path, events: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return path


class TestProcessFile:
    def test_all_on_time_when_no_prior_watermark(self, processor, tmp_path):
        events = [
            {"event_id": "e1", "event_time": T0.isoformat(), "feature1": 100},
            {
                "event_id": "e2",
                "event_time": (T0 + timedelta(seconds=5)).isoformat(),
                "feature1": 101,
            },
        ]
        path = write_ndjson(tmp_path / "raw_events" / "batch.ndjson", events)
        report = processor.process_file(path)

        assert report.total == 2
        assert len(report.on_time) == 2
        assert len(report.late) == 0

    def test_late_event_is_quarantined(self, processor, tmp_path, wm):
        # Establish watermark at T0+45s
        for i in range(10):
            wm.update(T0 + timedelta(seconds=i * 5))

        late_time = T0 - timedelta(seconds=60)
        events = [
            {
                "event_id": "e_good",
                "event_time": (T0 + timedelta(seconds=50)).isoformat(),
                "feature1": 99,
            },
            {
                "event_id": "e_bad",
                "event_time": late_time.isoformat(),
                "feature1": 10000,
            },
        ]
        path = write_ndjson(tmp_path / "raw_events" / "batch2.ndjson", events)
        report = processor.process_file(path)

        assert len(report.on_time) == 1
        assert len(report.late) == 1
        assert report.late[0]["feature1"] == 10000

    def test_report_run_id_matches_stem(self, processor, tmp_path):
        events = [{"event_id": "e1", "event_time": T0.isoformat(), "feature1": 100}]
        path = write_ndjson(tmp_path / "raw_events" / "my_run.ndjson", events)
        report = processor.process_file(path)
        assert report.run_id == "my_run"

    def test_final_watermark_is_set(self, processor, tmp_path):
        events = [
            {"event_id": "e1", "event_time": T0.isoformat(), "feature1": 100},
            {
                "event_id": "e2",
                "event_time": (T0 + timedelta(seconds=45)).isoformat(),
                "feature1": 102,
            },
        ]
        path = write_ndjson(tmp_path / "raw_events" / "batch3.ndjson", events)
        report = processor.process_file(path)
        expected_watermark = T0 + timedelta(seconds=45) - timedelta(seconds=DELAY)
        assert report.final_watermark == expected_watermark

    def test_empty_file_produces_empty_report(self, processor, tmp_path):
        path = tmp_path / "raw_events" / "empty.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
        report = processor.process_file(path)
        assert report.total == 0
        assert report.on_time == []
        assert report.late == []


class TestWriteOnTime:
    def test_creates_on_time_file(self, processor, tmp_path):
        events = [{"event_id": "e1", "event_time": T0.isoformat(), "feature1": 99}]
        path = processor.write_on_time(events, run_id="run_001")
        assert path.exists()
        assert "on_time" in path.name

    def test_on_time_file_is_valid_ndjson(self, processor, tmp_path):
        events = [
            {"event_id": "e1", "event_time": T0.isoformat(), "feature1": 99},
            {"event_id": "e2", "event_time": T0.isoformat(), "feature1": 101},
        ]
        path = processor.write_on_time(events, run_id="run_002")
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            assert "feature1" in json.loads(line)


class TestWriteQuarantine:
    def test_creates_quarantine_file(self, processor, tmp_path):
        events = [
            {"event_id": "e_bad", "event_time": T0.isoformat(), "feature1": 10000}
        ]
        path = processor.write_quarantine(events, run_id="run_001")
        assert path.exists()
        assert "quarantine" in path.name

    def test_quarantine_file_is_valid_ndjson(self, processor, tmp_path):
        events = [
            {"event_id": "e_bad", "event_time": T0.isoformat(), "feature1": 10000}
        ]
        path = processor.write_quarantine(events, run_id="run_003")
        obj = json.loads(path.read_text().strip())
        assert obj["feature1"] == 10000


class TestEndToEndScenario:
    """Simulate Run 1 (normal) then Run 2 (inject_late) to verify watermark persistence."""

    def test_run1_all_on_time_run2_quarantines_late(self, tmp_path):
        state_file = tmp_path / "watermarks" / "state.json"

        # --- Run 1: normal ---
        wm1 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm1.load_state()
        proc1 = StreamProcessor(watermark_manager=wm1, data_dir=tmp_path)

        run1_events = [
            {
                "event_id": f"e{i}",
                "event_time": (T0 + timedelta(seconds=i * 5)).isoformat(),
                "feature1": 100,
            }
            for i in range(10)
        ]
        run1_path = write_ndjson(tmp_path / "raw_events" / "run1.ndjson", run1_events)
        report1 = proc1.process_file(run1_path)
        wm1.save_state()

        assert report1.total == 10
        assert len(report1.on_time) == 10
        assert len(report1.late) == 0

        # max_event_time = T0+45s, watermark = T0+15s
        assert wm1.max_event_time == T0 + timedelta(seconds=45)

        # --- Run 2: inject_late ---
        wm2 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm2.load_state()
        proc2 = StreamProcessor(watermark_manager=wm2, data_dir=tmp_path)

        # Watermark boundary at T0+15s; late event is at T0-60s (75s behind)
        run2_events = [
            {
                "event_id": "e_ok1",
                "event_time": (T0 + timedelta(seconds=50)).isoformat(),
                "feature1": 99,
            },
            {
                "event_id": "e_ok2",
                "event_time": (T0 + timedelta(seconds=55)).isoformat(),
                "feature1": 98,
            },
            {
                "event_id": "e_ok3",
                "event_time": (T0 + timedelta(seconds=60)).isoformat(),
                "feature1": 102,
            },
            {
                "event_id": "e_bad",
                "event_time": (T0 - timedelta(seconds=60)).isoformat(),
                "feature1": 10000,
            },
        ]
        run2_path = write_ndjson(tmp_path / "raw_events" / "run2.ndjson", run2_events)
        report2 = proc2.process_file(run2_path)

        assert report2.total == 4
        assert len(report2.on_time) == 3
        assert len(report2.late) == 1
        assert report2.late[0]["feature1"] == 10000
