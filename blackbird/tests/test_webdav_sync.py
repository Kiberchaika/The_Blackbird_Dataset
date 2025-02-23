"""Tests for WebDAV sync functionality."""

import pytest
import os
import shutil
import tempfile
import requests
import time
import socket
from pathlib import Path
from typing import Optional
from unittest.mock import patch, MagicMock
from blackbird.webdav.setup import WebDAVSetup

class MockNginx:
    """Mock nginx server for testing."""
    
    def __init__(self, root_dir: Optional[Path] = None):
        self.root_dir = root_dir
        self.auth_enabled = False
        self.username = None
        self.password = None
        
    def handle_request(self, path: str, method: str = 'GET', auth: Optional[tuple] = None) -> requests.Response:
        """Handle a mock request."""
        response = requests.Response()
        
        # Ensure root directory is set and exists
        if not self.root_dir or not self.root_dir.exists():
            response.status_code = 500
            return response
        
        # Check auth if enabled
        if self.auth_enabled:
            if not auth or auth != (self.username, self.password):
                response.status_code = 401
                return response
        
        # Only handle GET requests for read-only access
        if method != 'GET':
            response.status_code = 405
            response._content = b"Only GET requests are supported"
            response.headers['Content-Type'] = 'text/plain'
            return response
            
        # Handle GET requests
        if path == '/' or path == '':
            # Directory listing
            response._content = '\n'.join(os.listdir(self.root_dir)).encode()
            response.status_code = 200
        else:
            # File download
            file_path = self.root_dir / path.lstrip('/')
            if file_path.exists() and file_path.is_file():
                response._content = file_path.read_bytes()
                response.status_code = 200
            else:
                response.status_code = 404
                
        return response

@pytest.fixture
def mock_nginx():
    """Create a mock nginx server."""
    def mock_request(url, *args, **kwargs):
        # Extract path and auth from request
        path = url.split('localhost:8081', 1)[1] if 'localhost:8081' in url else '/'
        auth = kwargs.get('auth')
        method = kwargs.get('method', 'GET').upper()  # Normalize method to uppercase
        
        # Handle request through mock nginx
        return nginx.handle_request(path, method, auth)
        
    # Create mock nginx instance
    nginx = MockNginx()  # Will be set in webdav_server fixture
    
    # Patch all request methods
    with patch('requests.get', side_effect=mock_request), \
         patch('requests.request', side_effect=mock_request):
        yield nginx

class TestWebDAVSync:
    """Test WebDAV sync functionality."""

    @pytest.fixture
    def mock_dataset(self):
        """Create a mock dataset for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_path = Path(temp_dir) / "test_dataset"
            
            # Create dataset structure
            artists = ["Artist1", "Artist2"]
            albums = ["Album1", "Album2"]
            components = [
                ("instrumental", "_instrumental.mp3"),
                ("vocals", "_vocals_noreverb.mp3"),
                ("mir", ".mir.json")
            ]
            
            # Create files
            for artist in artists:
                for album in albums:
                    album_path = dataset_path / artist / album
                    album_path.mkdir(parents=True)
                    
                    # Create track files
                    for track_num in range(1, 3):
                        base_name = f"track{track_num}"
                        for comp_name, suffix in components:
                            file_path = album_path / f"{base_name}{suffix}"
                            file_path.write_text(f"Mock {comp_name} content for {base_name}")
            
            yield dataset_path

    @pytest.fixture
    def webdav_server(self, mock_dataset, mock_nginx):
        """Set up WebDAV server for testing."""
        # Setup WebDAV
        port = 8081
        setup = WebDAVSetup(
            dataset_path=mock_dataset,
            port=port,
            username="testuser",
            password="testpass",
            non_interactive=True
        )
        
        # Configure mock nginx
        mock_nginx.root_dir = mock_dataset
        mock_nginx.auth_enabled = True
        mock_nginx.username = "testuser"
        mock_nginx.password = "testpass"
        
        # Mock sudo operations
        with patch("blackbird.webdav.system_ops.SystemOps.check_ubuntu", return_value=True), \
             patch("blackbird.webdav.system_ops.SystemOps.check_dependencies", return_value=(True, [])), \
             patch("blackbird.webdav.system_ops.SystemOps.check_system_resources",
                   return_value={"disk": True, "memory": True}), \
             patch("blackbird.webdav.system_ops.SystemOps.run_with_sudo", return_value=True), \
             patch("socket.socket") as mock_socket:
            
            # Mock socket for port check
            mock_sock = MagicMock()
            mock_sock.connect_ex.return_value = 0
            mock_socket.return_value = mock_sock
            
            # Run setup
            assert setup.run()
            
            yield {
                "url": f"http://localhost:{port}",
                "username": "testuser",
                "password": "testpass",
                "dataset_path": mock_dataset
            }
            
            # Cleanup
            setup._cleanup()

    def test_webdav_file_listing(self, webdav_server):
        """Test listing files via WebDAV."""
        response = requests.get(
            webdav_server["url"],
            auth=(webdav_server["username"], webdav_server["password"])
        )
        assert response.status_code == 200
        assert "Artist1" in response.text
        assert "Artist2" in response.text

    def test_webdav_file_download(self, webdav_server):
        """Test downloading files via WebDAV."""
        # Try downloading a specific file
        file_path = "Artist1/Album1/track1_instrumental.mp3"
        response = requests.get(
            f"{webdav_server['url']}/{file_path}",
            auth=(webdav_server["username"], webdav_server["password"])
        )
        assert response.status_code == 200
        assert "Mock instrumental content" in response.text

    def test_webdav_sync_dataset(self, webdav_server, tmp_path):
        """Test syncing dataset via WebDAV."""
        # Create destination directory
        dest_path = tmp_path / "synced_dataset"
        dest_path.mkdir()
        
        # Define files to sync
        files_to_sync = [
            "Artist1/Album1/track1_instrumental.mp3",
            "Artist1/Album1/track1_vocals_noreverb.mp3",
            "Artist1/Album1/track1.mir.json"
        ]
        
        # Download files
        for file_path in files_to_sync:
            # Create parent directories
            file_dest = dest_path / file_path
            file_dest.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            response = requests.get(
                f"{webdav_server['url']}/{file_path}",
                auth=(webdav_server["username"], webdav_server["password"])
            )
            assert response.status_code == 200
            
            # Save file
            file_dest.write_text(response.text)
            
        # Verify synced files
        for file_path in files_to_sync:
            synced_file = dest_path / file_path
            original_file = webdav_server["dataset_path"] / file_path
            
            assert synced_file.exists()
            assert synced_file.read_text() == original_file.read_text() 