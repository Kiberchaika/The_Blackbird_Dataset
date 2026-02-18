"""Tests for ProfilingStats helper used during sync."""

import pytest
from unittest.mock import patch, MagicMock

from blackbird.sync import ProfilingStats


class TestProfilingStats:
    """Tests for the ProfilingStats helper used during sync."""

    def test_initial_state(self):
        stats = ProfilingStats()
        summary = stats.get_summary()
        assert isinstance(summary, dict)
        assert len(summary) == 0

    def test_add_timing_and_summarize(self):
        stats = ProfilingStats()
        # add_timing takes nanoseconds
        stats.add_timing("download", 100_000_000)  # 100ms in ns
        stats.add_timing("download", 200_000_000)  # 200ms in ns
        stats.add_timing("index", 50_000_000)       # 50ms in ns

        summary = stats.get_summary()
        assert "download" in summary
        assert "index" in summary
        assert summary["download"]["calls"] == 2
        assert summary["download"]["total_ms"] == pytest.approx(300.0)
        assert summary["download"]["avg_ms"] == pytest.approx(150.0)
        assert summary["index"]["calls"] == 1

    def test_percentage_calculation(self):
        stats = ProfilingStats()
        stats.add_timing("fast_op", 25_000_000)   # 25ms
        stats.add_timing("slow_op", 75_000_000)   # 75ms

        summary = stats.get_summary()
        assert summary["fast_op"]["percentage"] == pytest.approx(25.0)
        assert summary["slow_op"]["percentage"] == pytest.approx(75.0)

    def test_single_operation(self):
        stats = ProfilingStats()
        stats.add_timing("only_op", 1_000_000)  # 1ms

        summary = stats.get_summary()
        assert summary["only_op"]["percentage"] == pytest.approx(100.0)
        assert summary["only_op"]["calls"] == 1
