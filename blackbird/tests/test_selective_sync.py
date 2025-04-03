import os
import logging
import itertools
import shutil
from pathlib import Path
from blackbird.sync import DatasetSync, SyncState
import pytest
from unittest.mock import MagicMock, patch
from blackbird.sync import SyncStats
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.dataset import Dataset

# Enable debug logging for blackbird module
logging.getLogger('blackbird').setLevel(logging.DEBUG)
logging.getLogger('webdav3').setLevel(logging.INFO)

# Test artist to use
TEST_ARTIST = "19_84"

# Local test directory where we'll sync to
LOCAL_TEST_DIR = "test_sync_data"

def list_directory_contents(path):
    """List contents of a directory, showing only first 5 items."""
    items = []
    for root, dirs, files in os.walk(path):
        level = root.replace(path, '').count(os.sep)
        indent = ' ' * 4 * level
        items.append(f"{indent}{os.path.basename(root)}/")
        if files:
            subindent = ' ' * 4 * (level + 1)
            for f in sorted(files)[:5]:  # Only show first 5 files
                items.append(f"{subindent}{f}")
    return items

def main():
    # Clean up any existing test data
    print("\nCleaning up existing test data...")
    if os.path.exists(LOCAL_TEST_DIR):
        print(f"Removing existing test directory: {LOCAL_TEST_DIR}")
        shutil.rmtree(LOCAL_TEST_DIR)
    
    # Create fresh test directory
    print("Creating fresh test directory...")
    os.makedirs(LOCAL_TEST_DIR)
    
    # Create sync manager
    sync = DatasetSync(Path(LOCAL_TEST_DIR))
    
    # Configure WebDAV client
    client = sync.configure_client(
        webdav_url="http://localhost:8080",
        username="user",
        password="test123"
    )
    
    # First sync instrumental files
    print("\nSyncing instrumental files...")
    sync.sync(
        client,
        components=['instrumental_audio'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=False  # Don't resume for first sync
    )
    
    # Then sync vocal files
    print("\nSyncing vocal files...")
    sync.sync(
        client,
        components=['vocals_audio'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=True  # Resume to preserve instrumental files
    )

    # Then sync mir files
    print("\nSyncing mir files...")
    sync.sync(
        client,
        components=['mir'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=True  # Resume to preserve instrumental files
    )
    
    # Print final directory structure
    print("\nFinal directory structure:")
    for item in list_directory_contents(os.path.join(LOCAL_TEST_DIR, TEST_ARTIST)):
        print(item)

@pytest.fixture
def test_dir(tmp_path):
    """Create a test directory with schema and index."""
    test_dir = tmp_path / "test_sync_data"
    test_dir.mkdir()
    
    # Create .blackbird directory
    blackbird_dir = test_dir / ".blackbird"
    blackbird_dir.mkdir()
    
    # Create schema
    schema = DatasetComponentSchema.create(test_dir)
    schema.schema["components"].update({
        "instrumental_audio": {
            "pattern": "*_instrumental.mp3",
            "required": True
        },
        "vocals_audio": {
            "pattern": "*_vocals_noreverb.mp3",
            "required": False
        },
        "mir": {
            "pattern": "*.mir.json",
            "required": False
        }
    })
    schema.save()
    
    # Create index
    index = DatasetIndex.create()
    
    # Add test tracks
    track1 = TrackInfo(
        track_path="19_84/Album1/Track1",
        artist="19_84",
        album_path="19_84/Album1",
        cd_number=None,
        base_name="Track1",
        files={
            "instrumental_audio": "19_84/Album1/Track1_instrumental.mp3",
            "vocals_audio": "19_84/Album1/Track1_vocals_noreverb.mp3",
            "mir": "19_84/Album1/Track1.mir.json"
        },
        file_sizes={
            "19_84/Album1/Track1_instrumental.mp3": 1000000,
            "19_84/Album1/Track1_vocals_noreverb.mp3": 800000,
            "19_84/Album1/Track1.mir.json": 5000
        }
    )
    
    track2 = TrackInfo(
        track_path="19_84/Album1/Track2",
        artist="19_84",
        album_path="19_84/Album1",
        cd_number=None,
        base_name="Track2",
        files={
            "instrumental_audio": "19_84/Album1/Track2_instrumental.mp3",
            "vocals_audio": "19_84/Album1/Track2_vocals_noreverb.mp3"
        },
        file_sizes={
            "19_84/Album1/Track2_instrumental.mp3": 1200000,
            "19_84/Album1/Track2_vocals_noreverb.mp3": 900000
        }
    )
    
    # Add tracks to index
    for track in [track1, track2]:
        index.tracks[track.track_path] = track
        index.track_by_album.setdefault(track.album_path, set()).add(track.track_path)
        index.album_by_artist.setdefault(track.artist, set()).add(track.album_path)
        index.total_size += sum(track.file_sizes.values())
    
    # Save index
    index.save(blackbird_dir / "index.pickle")
    
    return test_dir

@pytest.fixture
def mock_webdav_client():
    """Create a mock WebDAV client."""
    client = MagicMock()
    client.download_file = MagicMock()
    return client

def test_sync_initialization(test_dir):
    """Test sync manager initialization."""
    dataset = Dataset(test_dir)
    sync = DatasetSync(dataset)
    assert sync.dataset.path == test_dir
    assert sync.schema is not None
    assert sync.index is not None

def test_sync_with_invalid_component(test_dir, mock_webdav_client):
    """Test sync with invalid component."""
    dataset = Dataset(test_dir)
    sync = DatasetSync(dataset)
    with pytest.raises(ValueError, match="Invalid components"):
        sync.sync(mock_webdav_client, components=["nonexistent"])

def test_sync_specific_artist_and_components(test_dir, mock_webdav_client):
    """Test syncing specific components for a specific artist."""
    dataset = Dataset(test_dir)
    sync = DatasetSync(dataset)
    
    # Mock successful downloads
    def mock_download(remote_path, local_path, **kwargs):
        file_size = kwargs.get('file_size') # Get file_size from kwargs
        if file_size is None:
             raise ValueError("mock_download requires file_size keyword argument")
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(b'0' * file_size)
        return True 
    
    mock_webdav_client.download_file.side_effect = mock_download
    
    # Sync instrumental files
    stats = sync.sync(
        mock_webdav_client,
        components=["instrumental_audio"],
        artists=["19_84"]
    )
    
    assert stats.total_files == 2  # Two instrumental files
    assert stats.synced_files == 2
    assert stats.failed_files == 0
    
    # Verify the correct files were synced
    assert mock_webdav_client.download_file.call_count == 2
    calls = mock_webdav_client.download_file.call_args_list
    assert any("Track1_instrumental.mp3" in str(call) for call in calls)
    assert any("Track2_instrumental.mp3" in str(call) for call in calls)

def test_sync_resume(test_dir, mock_webdav_client):
    """Test resuming sync with existing files."""
    dataset = Dataset(test_dir)
    sync = DatasetSync(dataset)
    
    # Create an existing file with correct size
    existing_file = test_dir / "19_84/Album1/Track1_instrumental.mp3"
    existing_file.parent.mkdir(parents=True)
    with open(existing_file, 'wb') as f:
        f.write(b'0' * 1000000)  # Size from track1's file_sizes
    
    # Mock successful downloads
    def mock_download(remote_path, local_path, **kwargs):
        file_size = kwargs.get('file_size') # Get file_size from kwargs
        if file_size is None:
             raise ValueError("mock_download requires file_size keyword argument")
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(b'0' * file_size)
        return True 
    
    mock_webdav_client.download_file.side_effect = mock_download
    
    # Sync with resume
    stats = sync.sync(
        mock_webdav_client,
        components=["instrumental_audio"],
        artists=["19_84"],
        resume=True
    )
    
    assert stats.total_files == 2
    assert stats.synced_files == 1  # Only one file should be synced
    assert stats.skipped_files == 1  # One file should be skipped
    assert stats.failed_files == 0

def test_sync_error_handling(test_dir, mock_webdav_client):
    """Test handling of sync errors."""
    dataset = Dataset(test_dir)
    sync = DatasetSync(dataset)
    
    # Mock failed download
    def mock_download(remote_path, local_path, **kwargs):
        file_size = kwargs.get('file_size') # Get file_size from kwargs
        if "Track1" in remote_path:
            raise Exception("Download failed")
        if file_size is None:
             raise ValueError("mock_download requires file_size keyword argument")
        path = Path(local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(b'0' * file_size)
        return True 
    
    mock_webdav_client.download_file.side_effect = mock_download
    
    # Sync with some failures
    stats = sync.sync(
        mock_webdav_client,
        components=["instrumental_audio"],
        artists=["19_84"]
    )
    
    assert stats.total_files == 2
    assert stats.synced_files == 1
    assert stats.failed_files == 1
    assert stats.skipped_files == 0

if __name__ == "__main__":
    main() 