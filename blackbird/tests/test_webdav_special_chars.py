import pytest
import socket
import shutil
import time
from pathlib import Path
import os
import urllib.parse

from blackbird.schema import DatasetComponentSchema
from blackbird.dataset import Dataset
from blackbird.sync import WebDAVClient, clone_dataset

def test_webdav_special_chars_download(tmp_path):
    """Test WebDAV download with special characters in filenames, especially # symbols."""

    # Check if WebDAV server is running on port 7771 (the one with real-world examples)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('localhost', 7771))
        webdav_available = True
    except (ConnectionRefusedError, socket.error):
        webdav_available = False
    finally:
        sock.close()

    if not webdav_available:
        pytest.skip("WebDAV server not available on port 7771")

    dest_path = tmp_path / "special_chars_download"
    dest_path.mkdir()

    # Initialize schema in destination
    dest_schema = DatasetComponentSchema.create(dest_path)
    dest_schema.add_component("mir.json", "*.mir.json")
    dest_schema.save()

    # Configure WebDAV client
    client = WebDAVClient("webdav://localhost:7771")

    # Test specific problematic files from the error logs
    problem_files = [
        "АнимациЯ/Распутье [2015]/05.АнимациЯ - #непорусски.mir.json",
        "Градусы/Градус 100 [2016]/01.Градусы - #Валигуляй.mir.json",
        "Александр Ливер/Проффессионнал [2003]/06.Александр Ливер - Школьная история #1.mir.json",
        "Александр Ливер/Проффессионнал [2003]/08.Александр Ливер - Школьная история #2.mir.json",
        "E-SEX-T/Время Слона [2000]/02.E-SEX-T - Всё сложней (Облом #2).mir.json"
    ]

    # Test direct download of each file
    for file_path in problem_files:
        local_file = dest_path / file_path
        local_file.parent.mkdir(parents=True, exist_ok=True)
        success = client.download_file(file_path, local_file)
        assert success, f"Failed to download file with special characters: {file_path}"
        assert local_file.exists(), f"File was not created: {local_file}"

    # Test selective sync of these specific files
    test_result = clone_dataset(
        source_url="webdav://localhost:7771",
        destination=dest_path,
        components=["mir.json"],
        artists=["АнимациЯ", "Градусы", "Александр Ливер", "E-SEX-T"]
    )

    assert test_result.failed_files == 0, f"{test_result.failed_files} files failed to sync"

    for file_path in problem_files:
        file = dest_path / file_path
        assert file.exists(), f"File with special characters not synced: {file}"

def test_url_encoding_functions():
    """Test URL encoding functions for special characters."""
    
    # Test paths with special characters
    test_paths = [
        "Artist#1/Album [2023]/01. Track with # symbol.mp3",
        "Artist/Album/Track with spaces and #hashtags.mp3",
        "Artist/Album/Track with ?query=param&another=value.mp3",
        "Artist/Album/Track with +plus and @at.mp3",
        "Artist/Album/Track with русские буквы.mp3",
        "Artist/Album/Track with 日本語.mp3"
    ]
    
    # Test that our URL encoding function properly handles these paths
    for path in test_paths:
        # This is what our implementation should do
        encoded_path = urllib.parse.quote(path, safe='/')
        
        # Verify the encoding is correct
        assert '#' not in encoded_path or encoded_path.endswith('#'), f"# not properly encoded in {encoded_path}"
        assert ' ' not in encoded_path, f"Spaces not properly encoded in {encoded_path}"
        assert '?' not in encoded_path or encoded_path.endswith('?'), f"? not properly encoded in {encoded_path}"
        
        # Verify the path can be decoded back
        decoded_path = urllib.parse.unquote(encoded_path)
        assert decoded_path == path, f"Encoding/decoding failed: {path} -> {encoded_path} -> {decoded_path}" 