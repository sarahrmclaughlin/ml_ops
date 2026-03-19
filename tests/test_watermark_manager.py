import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from watermark_manager import WatermarkManager


T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
DELAY = 30  # seconds


@pytest.fixture
def state_file(tmp_path):
    return tmp_path / "watermark_state.json"


@pytest.fixture
def wm(state_file):
    return WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)


class TestInitialState:
    def test_no_watermark_before_first_event(self, wm):
        assert wm.get_watermark() is None

    def test_no_event_is_late_before_watermark(self, wm):
        assert wm.is_late(T0) is False

    def test_describe_with_no_state(self, wm):
        desc = wm.describe()
        assert "None" in desc
        assert "delay=30s" in desc


class TestUpdate:
    def test_update_sets_max_event_time(self, wm):
        wm.update(T0)
        assert wm.max_event_time == T0

    def test_update_advances_on_newer_event(self, wm):
        wm.update(T0)
        t1 = T0 + timedelta(seconds=10)
        wm.update(t1)
        assert wm.max_event_time == t1

    def test_update_does_not_lower_on_late_event(self, wm):
        wm.update(T0)
        old = T0 - timedelta(seconds=60)
        wm.update(old)
        assert wm.max_event_time == T0


class TestWatermarkBoundary:
    def test_watermark_equals_max_minus_delay(self, wm):
        wm.update(T0)
        expected = T0 - timedelta(seconds=DELAY)
        assert wm.get_watermark() == expected

    def test_on_time_event_not_late(self, wm):
        wm.update(T0)
        on_time = T0  # equal to max → definitely on-time
        assert wm.is_late(on_time) is False

    def test_event_exactly_at_watermark_not_late(self, wm):
        wm.update(T0)
        at_boundary = T0 - timedelta(seconds=DELAY)
        assert wm.is_late(at_boundary) is False

    def test_event_just_behind_watermark_is_late(self, wm):
        wm.update(T0)
        late = T0 - timedelta(seconds=DELAY + 1)
        assert wm.is_late(late) is True

    def test_very_old_event_is_late(self, wm):
        wm.update(T0)
        ancient = T0 - timedelta(seconds=3600)
        assert wm.is_late(ancient) is True


class TestPersistence:
    def test_save_and_load_round_trips(self, wm, state_file):
        wm.update(T0)
        wm.save_state()

        wm2 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm2.load_state()
        assert wm2.max_event_time == T0

    def test_save_creates_parent_dirs(self, tmp_path, wm):
        deep_path = tmp_path / "a" / "b" / "state.json"
        wm2 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=deep_path)
        wm2.update(T0)
        wm2.save_state()
        assert deep_path.exists()

    def test_load_with_no_file_leaves_state_none(self, state_file):
        wm = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm.load_state()
        assert wm.max_event_time is None

    def test_saved_json_contains_delay(self, wm, state_file):
        wm.update(T0)
        wm.save_state()
        data = json.loads(state_file.read_text())
        assert data["watermark_delay_seconds"] == DELAY

    def test_watermark_advances_across_runs(self, state_file):
        """Simulate two DAG runs: second run loads state from first."""
        # Run 1
        wm1 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm1.load_state()
        for i in range(10):
            wm1.update(T0 + timedelta(seconds=i * 5))
        wm1.save_state()

        # Run 2
        wm2 = WatermarkManager(watermark_delay_seconds=DELAY, state_file=state_file)
        wm2.load_state()
        assert wm2.max_event_time == T0 + timedelta(seconds=45)
        assert wm2.get_watermark() == T0 + timedelta(seconds=15)
