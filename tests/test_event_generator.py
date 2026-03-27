import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from event_generator import EventGenerator

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def gen(tmp_path):
    return EventGenerator(data_dir=tmp_path, seed=42)


class TestGenerateNormalBatch:
    def test_returns_correct_count(self, gen):
        events = gen.generate_normal_batch(10, T0)
        assert len(events) == 10

    def test_event_times_spaced_by_interval(self, gen):
        events = gen.generate_normal_batch(5, T0, interval_seconds=5)
        times = [datetime.fromisoformat(e["event_time"]) for e in events]
        assert times[0] == T0
        for i in range(1, len(times)):
            assert (times[i] - times[i - 1]).total_seconds() == 5

    def test_custom_interval(self, gen):
        events = gen.generate_normal_batch(3, T0, interval_seconds=10)
        times = [datetime.fromisoformat(e["event_time"]) for e in events]
        assert (times[1] - times[0]).total_seconds() == 10

    def test_feature1_in_reasonable_range(self, gen):
        events = gen.generate_normal_batch(100, T0)
        for e in events:
            assert 70 <= e["feature1"] <= 130, f"Unexpected feature1={e['feature1']}"

    def test_each_event_has_required_keys(self, gen):
        events = gen.generate_normal_batch(3, T0)
        for e in events:
            assert "event_id" in e
            assert "event_time" in e
            assert "feature1" in e

    def test_event_ids_are_unique(self, gen):
        events = gen.generate_normal_batch(20, T0)
        ids = [e["event_id"] for e in events]
        assert len(ids) == len(set(ids))


class TestGenerateLateEvent:
    def test_feature1_is_anomalous(self, gen):
        event = gen.generate_late_event(late_by_seconds=60, current_max_event_time=T0)
        assert event["feature1"] == 10000

    def test_event_time_offset_from_max(self, gen):
        event = gen.generate_late_event(late_by_seconds=75, current_max_event_time=T0)
        event_time = datetime.fromisoformat(event["event_time"])
        assert event_time == T0 - timedelta(seconds=75)

    def test_has_required_keys(self, gen):
        event = gen.generate_late_event(late_by_seconds=60, current_max_event_time=T0)
        assert "event_id" in event
        assert "event_time" in event
        assert "feature1" in event


class TestWriteBatch:
    def test_file_is_created(self, gen, tmp_path):
        events = gen.generate_normal_batch(3, T0)
        path = gen.write_batch(events)
        assert path.exists()

    def test_file_is_ndjson(self, gen, tmp_path):
        events = gen.generate_normal_batch(3, T0)
        path = gen.write_batch(events)
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3
        for line in lines:
            obj = json.loads(line)
            assert "feature1" in obj

    def test_output_dir_is_raw_events(self, gen, tmp_path):
        events = gen.generate_normal_batch(1, T0)
        path = gen.write_batch(events)
        assert path.parent.name == "raw_events"

    def test_creates_parent_dirs(self, tmp_path):
        deep_data = tmp_path / "a" / "b"
        gen2 = EventGenerator(data_dir=deep_data, seed=0)
        events = gen2.generate_normal_batch(1, T0)
        path = gen2.write_batch(events)
        assert path.exists()
