"""Tests for WebDAV setup functionality."""

import pytest
import os
import socket
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from blackbird.webdav.setup import WebDAVSetup, WebDAVShare

class TestWebDAVShare:
    """Test WebDAV share information handling."""

    def test_from_config_valid(self, tmp_path):
        """Test creating share info from valid config."""
        config_content = """
server {
    listen 8081;
    server_name _;
    root /path/to/dataset;
}
"""
        config_path = tmp_path / "test.conf"
        config_path.write_text(config_content)
        
        with patch("pathlib.Path.exists", return_value=True):
            share = WebDAVShare.from_config(config_path)
            assert share is not None
            assert share.port == 8081
            assert share.path == "/path/to/dataset"
            assert share.enabled is True

    def test_from_config_invalid(self, tmp_path):
        """Test handling invalid config file."""
        config_content = "invalid config"
        config_path = tmp_path / "test.conf"
        config_path.write_text(config_content)
        
        share = WebDAVShare.from_config(config_path)
        assert share is None

    def test_is_running_active(self):
        """Test checking active share."""
        share = WebDAVShare(port=80, path="/test", config_path="/test.conf", enabled=True)
        
        # Mock socket to simulate port in use
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        
        with patch("socket.socket", return_value=mock_sock):
            assert share.is_running() is True

    def test_is_running_inactive(self):
        """Test checking inactive share."""
        share = WebDAVShare(port=8081, path="/test", config_path="/test.conf", enabled=True)
        
        # Mock socket to simulate port not in use
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 1
        
        with patch("socket.socket", return_value=mock_sock):
            assert share.is_running() is False

class TestWebDAVSetup:
    """Test WebDAV setup functionality."""

    @pytest.fixture
    def setup_wizard(self, tmp_path):
        """Create a WebDAVSetup instance for testing."""
        return WebDAVSetup(
            dataset_path=tmp_path / "test_dataset",
            port=8081,
            username="testuser",
            password="testpass",
            non_interactive=True
        )

    def test_check_ubuntu_true(self, setup_wizard):
        """Test Ubuntu detection when true."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_ubuntu", return_value=True):
            assert setup_wizard._check_ubuntu() is True

    def test_check_ubuntu_false(self, setup_wizard):
        """Test Ubuntu detection when false."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_ubuntu", return_value=False):
            assert setup_wizard._check_ubuntu() is False

    def test_ensure_dependencies_all_present(self, setup_wizard):
        """Test dependency check when all present."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_dependencies", 
                  return_value=(True, [])):
            assert setup_wizard._ensure_dependencies() is True

    def test_ensure_dependencies_missing_non_interactive(self, setup_wizard):
        """Test dependency check with missing packages in non-interactive mode."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_dependencies", 
                  return_value=(False, ["nginx"])):
            assert setup_wizard._ensure_dependencies() is False

    def test_check_system_resources_sufficient(self, setup_wizard):
        """Test system resource check when sufficient."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_system_resources",
                  return_value={"disk": True, "memory": True}):
            assert setup_wizard._check_system_resources() is True

    def test_check_system_resources_insufficient_non_interactive(self, setup_wizard):
        """Test system resource check when insufficient in non-interactive mode."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_system_resources",
                  return_value={"disk": False, "memory": True}):
            assert setup_wizard._check_system_resources() is False

    def test_run_success(self, setup_wizard):
        """Test successful setup run."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_ubuntu", return_value=True), \
             patch("blackbird.webdav.system_ops.SystemOps.check_dependencies", return_value=(True, [])), \
             patch("blackbird.webdav.system_ops.SystemOps.check_system_resources",
                   return_value={"disk": True, "memory": True}), \
             patch("blackbird.webdav.config_gen.ConfigGenerator.apply", return_value=True):
            assert setup_wizard.run() is True

    def test_run_failure_dependencies(self, setup_wizard):
        """Test setup failure due to dependencies."""
        with patch("blackbird.webdav.system_ops.SystemOps.check_ubuntu", return_value=True), \
             patch("blackbird.webdav.system_ops.SystemOps.check_dependencies", 
                   return_value=(False, ["nginx"])):
            assert setup_wizard.run() is False

    def test_list_shares_empty(self):
        """Test listing shares when none exist."""
        with patch("pathlib.Path.exists", return_value=False):
            shares = WebDAVSetup.list_shares()
            assert len(shares) == 0

    def test_list_shares_found(self, tmp_path):
        """Test listing existing shares."""
        config_content = """
server {
    listen 8081;
    server_name _;
    root /path/to/dataset;
}
"""
        # Create mock config file
        config_path = tmp_path / "blackbird-webdav-8081.conf"
        config_path.write_text(config_content)
        
        with patch("pathlib.Path.exists", return_value=True), \
             patch("pathlib.Path.glob", return_value=[config_path]):
            shares = WebDAVSetup.list_shares()
            assert len(shares) == 1
            assert shares[0].port == 8081
            assert shares[0].path == "/path/to/dataset" 