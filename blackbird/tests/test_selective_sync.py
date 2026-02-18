from pathlib import Path
from blackbird.sync import DatasetSync
import pytest
from unittest.mock import MagicMock
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.dataset import Dataset


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
    for i, track in enumerate([track1, track2]):
        # Add location prefix to file paths for consistency
        files_with_prefix = {comp: f"Main/{path}" for comp, path in track.files.items()}
        filesizes_with_prefix = {f"Main/{path}": size for path, size in track.file_sizes.items()}
        track_info = TrackInfo(
            track_path=f"Main/{track.track_path}",
            artist=track.artist,
            album_path=f"Main/{track.album_path}",
            cd_number=track.cd_number,
            base_name=track.base_name,
            files=files_with_prefix, # Use prefixed paths
            file_sizes=filesizes_with_prefix # Use prefixed paths
        )
        index.tracks[f"Main/{track.track_path}"] = track_info
        # Update index lookups with prefixed paths
        index.track_by_album.setdefault(f"Main/{track.album_path}", set()).add(f"Main/{track.track_path}")
        index.album_by_artist.setdefault(track.artist, set()).add(f"Main/{track.album_path}")
        index.total_size += sum(track.file_sizes.values())
    
    # Save index
    index.save(blackbird_dir / "index.pickle")
    
    return test_dir

@pytest.fixture
def mock_webdav_client(test_dir):
    """Create a mock WebDAV client with mocked index and schema."""
    client = MagicMock()
    client.download_file = MagicMock()

    # Load the actual index and schema from the test directory
    dataset = Dataset(test_dir)
    # Ensure schema and index are loaded/created if they weren't already
    # dataset.load_schema() # schema is loaded in Dataset.__init__
    # dataset.load_index() # index is loaded in Dataset.__init__

    # Mock get_index and get_schema to return the loaded objects
    client.get_index = MagicMock(return_value=dataset.index)
    client.get_schema = MagicMock(return_value=dataset.schema)
    
    # Mock base_url and options needed for state file creation
    client.base_url = "http://mock-server"
    client.client = MagicMock()
    client.client.options = {'webdav_root': '/mock_root/'}

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
    with pytest.raises(ValueError, match="Component 'nonexistent' not found in remote schema."):
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
    assert stats.skipped_files == 1  # One file should be skipped
    assert stats.downloaded_files == 1 # The other file should be downloaded
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
