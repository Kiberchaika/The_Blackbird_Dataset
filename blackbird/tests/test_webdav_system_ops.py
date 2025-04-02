"""Tests for WebDAV system operations."""

import pytest
import os
import subprocess
from unittest.mock import patch, MagicMock
from blackbird.webdav.system_ops import SystemOps

class TestSystemOps:
    """Test system-level operations for WebDAV setup."""

    def test_check_ubuntu_with_file(self, tmp_path):
        """Test Ubuntu detection with lsb-release file."""
        # Create mock lsb-release file
        lsb_file = tmp_path / "lsb-release"
        lsb_file.write_text("DISTRIB_ID=Ubuntu\nDISTRIB_RELEASE=20.04")
        
        with patch("builtins.open", return_value=open(lsb_file)):
            assert SystemOps.check_ubuntu() is True

    def test_check_ubuntu_without_file(self):
        """Test Ubuntu detection without lsb-release file."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            assert SystemOps.check_ubuntu() is False

    def test_check_dependencies_all_present(self):
        """Test dependency checking when all dependencies are present."""
        with patch("shutil.which", return_value="/usr/sbin/nginx"), \
             patch("os.path.exists", return_value=True):
            installed, missing = SystemOps.check_dependencies()
            assert installed is True
            assert len(missing) == 0

    def test_check_dependencies_missing(self):
        """Test dependency checking with missing dependencies."""
        mock_dpkg = MagicMock(stdout="", returncode=1)  # dpkg returns non-zero when package not found
        with patch("subprocess.run", return_value=mock_dpkg), \
             patch("shutil.which", return_value=None), \
             patch("os.path.exists", return_value=False):
            installed, missing = SystemOps.check_dependencies()
            assert installed is False
            assert len(missing) == 2
            assert "nginx" in missing
            assert "libnginx-mod-http-dav-ext" in missing

    def test_install_dependencies_success(self):
        """Test successful package installation."""
        mock_run = MagicMock(return_value=MagicMock(returncode=0))
        with patch("subprocess.run", mock_run):
            assert SystemOps.install_dependencies(["nginx"]) is True
            # Verify apt-get update and install were called
            assert mock_run.call_count >= 2

    def test_install_dependencies_no_sudo(self):
        """Test package installation without sudo access."""
        def mock_run(cmd, *args, **kwargs):
            if cmd[0] == "sudo" and cmd[1] == "-n":
                return MagicMock(returncode=1)
            return MagicMock(returncode=0)
            
        with patch("subprocess.run", side_effect=mock_run):
            assert SystemOps.install_dependencies(["nginx"], non_interactive=True) is False

    def test_check_system_resources_sufficient(self):
        """Test system resource checking with sufficient resources."""
        mock_disk = MagicMock(free=200 * 1024 * 1024)  # 200MB free
        mock_memory = MagicMock(available=100 * 1024 * 1024)  # 100MB available
        
        with patch("psutil.disk_usage", return_value=mock_disk), \
             patch("psutil.virtual_memory", return_value=mock_memory):
            resources = SystemOps.check_system_resources()
            assert resources["disk"] is True
            assert resources["memory"] is True

    def test_check_system_resources_insufficient(self):
        """Test system resource checking with insufficient resources."""
        mock_disk = MagicMock(free=50 * 1024 * 1024)  # 50MB free
        mock_memory = MagicMock(available=25 * 1024 * 1024)  # 25MB available
        
        with patch("psutil.disk_usage", return_value=mock_disk), \
             patch("psutil.virtual_memory", return_value=mock_memory):
            resources = SystemOps.check_system_resources()
            assert resources["disk"] is False
            assert resources["memory"] is False

    def test_check_system_resources_error_handling(self):
        """Test system resource checking error handling."""
        with patch("psutil.disk_usage", side_effect=Exception("Test error")):
            resources = SystemOps.check_system_resources()
            assert resources["disk"] is False
            assert resources["memory"] is False 