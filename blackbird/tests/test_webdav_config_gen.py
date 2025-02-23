"""Tests for WebDAV configuration generation."""

import pytest
import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
from blackbird.webdav.config_gen import ConfigGenerator

class TestConfigGenerator:
    """Test WebDAV configuration generation."""

    @pytest.fixture
    def config_gen(self, tmp_path):
        """Create a ConfigGenerator instance for testing."""
        return ConfigGenerator(
            dataset_path=tmp_path / "test_dataset",
            port=8081,
            username="testuser",
            password="testpass"
        )

    @pytest.fixture
    def config_gen_no_auth(self, tmp_path):
        """Create a ConfigGenerator instance without auth for testing."""
        return ConfigGenerator(
            dataset_path=tmp_path / "test_dataset",
            port=8081
        )

    def test_generate_config_with_auth(self, config_gen):
        """Test generating nginx config with authentication."""
        config = config_gen.generate()
        
        # Check basic server config
        assert "listen 8081;" in config
        assert "server_name _;" in config
        assert str(config_gen.dataset_path) in config
        
        # Check WebDAV config
        assert "dav_methods" in config
        assert "dav_ext_methods" in config
        assert "limit_except GET PROPFIND OPTIONS" in config
        
        # Check auth config
        assert "auth_basic" in config
        assert "auth_basic_user_file" in config
        assert f".htpasswd_{config_gen.config_name}" in config

    def test_generate_config_without_auth(self, config_gen_no_auth):
        """Test generating nginx config without authentication."""
        config = config_gen_no_auth.generate()
        
        # Check basic config present
        assert "listen 8081;" in config
        assert "server_name _;" in config
        
        # Check auth config absent
        assert "auth_basic" not in config
        assert "auth_basic_user_file" not in config

    def test_setup_auth_success(self, config_gen, tmp_path):
        """Test successful auth setup."""
        with patch("pathlib.Path.exists", return_value=False), \
             patch("subprocess.run", return_value=MagicMock(returncode=0)) as mock_run:
            assert config_gen._setup_auth() is True
            
            # Verify auth file was created and moved
            mv_calls = [call for call in mock_run.call_args_list 
                       if "mv" in str(call)]
            assert len(mv_calls) == 1
            assert f".htpasswd_{config_gen.config_name}" in str(mv_calls[0])
            
            # Verify permissions were set
            chmod_calls = [call for call in mock_run.call_args_list 
                         if "chmod" in str(call)]
            assert len(chmod_calls) == 1
            assert "600" in str(chmod_calls[0])

    def test_setup_auth_failure(self, config_gen):
        """Test auth setup failure."""
        with patch("subprocess.run", side_effect=Exception("Test error")):
            assert config_gen._setup_auth() is False

    def test_apply_config_success(self, config_gen):
        """Test successful config application."""
        mock_process = MagicMock()
        mock_process.returncode = 0
        
        with patch("subprocess.Popen", return_value=mock_process) as mock_popen, \
             patch("subprocess.run", return_value=MagicMock(returncode=0)) as mock_run:
            assert config_gen.apply() is True
            
            # Verify config was written
            assert mock_popen.called
            assert config_gen.config_name in str(mock_popen.call_args[0][0])
            
            # Verify nginx was tested and reloaded
            nginx_calls = [call for call in mock_run.call_args_list 
                         if "nginx" in str(call)]
            assert len(nginx_calls) >= 2

    def test_apply_config_nginx_test_failure(self, config_gen):
        """Test config application with nginx test failure."""
        def mock_run_cmd(cmd, *args, **kwargs):
            if "nginx -t" in " ".join(cmd):
                return MagicMock(returncode=1)
            return MagicMock(returncode=0)
        
        with patch("subprocess.Popen", return_value=MagicMock(returncode=0)), \
             patch("subprocess.run", side_effect=mock_run_cmd):
            assert config_gen.apply() is False

    def test_remove_config_success(self, config_gen):
        """Test successful config removal."""
        with patch("pathlib.Path.exists", return_value=True), \
             patch("subprocess.run", return_value=MagicMock(returncode=0)) as mock_run:
            assert config_gen.remove() is True
            
            # Verify files were removed
            rm_calls = [call for call in mock_run.call_args_list 
                       if "rm" in str(call)]
            assert len(rm_calls) == 3  # config, symlink, and auth file
            
            # Verify nginx was reloaded
            reload_calls = [call for call in mock_run.call_args_list 
                          if "reload" in str(call)]
            assert len(reload_calls) == 1

    def test_remove_config_failure(self, config_gen):
        """Test config removal failure."""
        with patch("pathlib.Path.exists", return_value=True), \
             patch("subprocess.run", side_effect=Exception("Test error")):
            assert config_gen.remove() is False 