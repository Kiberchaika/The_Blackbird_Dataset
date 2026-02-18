"""Tests for sync configuration options — verifying that clone_dataset and
configure_client correctly handle parallel, HTTP/2, and connection pool settings."""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from blackbird.sync import WebDAVClient, configure_client, ProfilingStats


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


class TestConfigureClient:
    """Tests for configure_client with various configuration options."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_default_pool_size(self, mock_session_cls, mock_client_cls):
        client = configure_client("webdav://localhost:7771")
        assert client.connection_pool_size == 10

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_custom_pool_size(self, mock_session_cls, mock_client_cls):
        client = configure_client("webdav://localhost:7771", connection_pool_size=50)
        assert client.connection_pool_size == 50

    @patch("blackbird.sync.httpx", create=True)
    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_http2_flag_stored(self, mock_session_cls, mock_client_cls, mock_httpx):
        """HTTP/2 preference is stored on the client."""
        # Mock httpx so it doesn't try to create a real HTTP/2 client
        mock_httpx.Client.return_value = MagicMock()
        with patch("blackbird.sync.HTTPX_AVAILABLE", True):
            client = configure_client("webdav://localhost:7771", use_http2=True)
            assert client.use_http2 is True

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_http2_disabled_without_httpx(self, mock_session_cls, mock_client_cls):
        """HTTP/2 should be False when httpx is not available."""
        with patch("blackbird.sync.HTTPX_AVAILABLE", False):
            client = configure_client("webdav://localhost:7771", use_http2=True)
            assert client.use_http2 is False

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_session_has_retry_adapter(self, mock_session_cls, mock_client_cls):
        """Session should be configured with retry logic."""
        client = configure_client("webdav://localhost:7771")
        # The session.mount should have been called for http:// and https://
        assert client.session.mount.call_count >= 2


class TestWebDAVClientParallel:
    """Tests verifying that parallel download settings are properly configured."""

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_pool_connections_match_config(self, mock_session_cls, mock_client_cls):
        """HTTPAdapter should be created with the specified pool size."""
        with patch("blackbird.sync.HTTPAdapter") as mock_adapter:
            WebDAVClient("webdav://localhost:7771", connection_pool_size=25)
            mock_adapter.assert_called_with(
                pool_connections=25,
                pool_maxsize=25,
                max_retries=mock_adapter.call_args[1]["max_retries"]
            )

    @patch("blackbird.sync.Client")
    @patch("blackbird.sync.requests.Session")
    def test_different_pool_sizes(self, mock_session_cls, mock_client_cls):
        """Different pool sizes should produce different configurations."""
        with patch("blackbird.sync.HTTPAdapter") as mock_adapter:
            WebDAVClient("webdav://localhost:7771", connection_pool_size=5)
            first_call = mock_adapter.call_args_list[-1]

            WebDAVClient("webdav://localhost:7771", connection_pool_size=30)
            second_call = mock_adapter.call_args_list[-1]

            assert first_call[1]["pool_connections"] != second_call[1]["pool_connections"]
