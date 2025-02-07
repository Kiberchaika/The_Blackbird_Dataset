import pytest
from pathlib import Path
import shutil
import json
from datetime import datetime
from unittest.mock import MagicMock, patch
from webdav3.client import Client
from blackbird.sync import DatasetSync, SyncState
from blackbird.schema import DatasetComponentSchema

@pytest.fixture
def mock_webdav():
    """Create a mock WebDAV client that tracks operations."""
    client = MagicMock(spec=Client)
    
    # Track operations
    client._downloaded_files = {}
    client._remote_files = set()  # Files available on remote
    client._remote_schema = None  # Remote schema content
    
    def mock_download(remote_path, local_path):
        """Track file downloads."""
        if remote_path == ".blackbird/schema.json":
            # Write remote schema to local path
            if client._remote_schema:
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'w') as f:
                    json.dump(client._remote_schema, f)
            else:
                raise Exception("Remote schema not found")
        else:
            if remote_path not in client._remote_files:
                raise Exception(f"File not found: {remote_path}")
            client._downloaded_files[remote_path] = local_path
            # Create the file locally
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).touch()
            
    def mock_list(pattern):
        """Return list of matching remote files."""
        import fnmatch
        return [f for f in client._remote_files if fnmatch.fnmatch(f, pattern)]
    
    client.download_sync = mock_download
    client.list = mock_list
    
    return client

@pytest.fixture
def destination_dataset(tmp_path):
    """Create a destination dataset with initial schema."""
    dataset_root = tmp_path / "dest_dataset"
    
    # Create schema with basic components
    schema = DatasetComponentSchema.create(dataset_root)
    schema.schema["components"].update({
        "instrumental": {
            "pattern": "*_instrumental.mp3",
            "required": False,
            "description": "Instrumental tracks"
        }
    })
    schema.save()
    
    yield dataset_root
    shutil.rmtree(dataset_root)

def test_schema_update_from_remote(destination_dataset, mock_webdav):
    """Test updating schema from remote.
    
    This test verifies that:
    1. Remote schema is downloaded
    2. Local schema is updated with remote components
    3. Only requested components are updated if specified
    4. Files are synced using updated schema patterns
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema with additional components
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            },
            "vocals": {
                "pattern": "*_vocals_noreverb.mp3",
                "required": False,
                "description": "Isolated vocals"
            },
            "mir": {
                "pattern": "*.mir.json",
                "required": False
            }
        },
        "sync": {
            "default_components": ["instrumental", "vocals"]
        }
    }
    
    # Setup mock remote files
    remote_files = [
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track1_vocals_noreverb.mp3",
        "Artist1/Album1/track1.mir.json"
    ]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Sync only vocals component
    state = sync.sync_from_remote(
        mock_webdav,
        components=["vocals"]
    )
    
    # Verify schema was updated
    assert "vocals" in sync.schema.schema["components"]
    assert sync.schema.schema["components"]["vocals"]["description"] == "Isolated vocals"
    assert "mir" not in sync.schema.schema["components"]  # Not requested
    
    # Verify only vocals files were downloaded
    downloaded = set(mock_webdav._downloaded_files.keys())
    assert len(downloaded) == 1
    assert all("_vocals_noreverb.mp3" in f for f in downloaded)

def test_sync_with_missing_component(destination_dataset, mock_webdav):
    """Test syncing with component not in remote schema.
    
    This test verifies that:
    1. Warning is logged for missing components
    2. Sync continues with available components
    3. No files are downloaded for missing components
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            }
        }
    }
    
    # Setup mock remote files
    remote_files = ["Artist1/Album1/track1_instrumental.mp3"]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Try to sync non-existent component
    with pytest.warns(UserWarning, match="Component nonexistent not found in remote schema"):
        state = sync.sync_from_remote(
            mock_webdav,
            components=["instrumental", "nonexistent"]
        )
    
    # Verify only instrumental files were downloaded
    downloaded = set(mock_webdav._downloaded_files.keys())
    assert len(downloaded) == 1
    assert all("_instrumental.mp3" in f for f in downloaded)

def test_selective_component_pull(destination_dataset, mock_webdav):
    """Test pulling only specific components.
    
    This test verifies that:
    1. We can selectively pull only certain components
    2. Only requested components are downloaded
    3. Sync state tracks downloads correctly
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            },
            "vocals": {
                "pattern": "*_vocals_noreverb.mp3",
                "required": False
            },
            "mir": {
                "pattern": "*.mir.json",
                "required": False
            }
        }
    }
    
    # Setup mock remote with various files
    remote_files = [
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track1_vocals_noreverb.mp3",
        "Artist1/Album1/track1.mir.json"
    ]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Pull only instrumental files
    state = sync.sync_from_remote(
        mock_webdav,
        components=["instrumental"]
    )
    
    # Verify only instrumental files were downloaded
    downloaded = set(mock_webdav._downloaded_files.keys())
    assert len(downloaded) == 1
    assert all("_instrumental.mp3" in f for f in downloaded)
    
    # Verify sync state
    assert len(state.synced_files) == 1
    assert not state.failed_files

def test_resume_sync(destination_dataset, mock_webdav):
    """Test resuming a sync operation.
    
    This test verifies that:
    1. Sync can be resumed from previous state
    2. Already synced files are skipped
    3. Progress is properly tracked
    4. State is saved after each file
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            }
        }
    }
    
    # Setup mock remote with multiple files
    remote_files = [
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track2_instrumental.mp3"
    ]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Create initial sync state with one file already synced
    initial_state = SyncState.create_new()
    initial_state.synced_files.add("Artist1/Album1/track1_instrumental.mp3")
    initial_state.completed_bytes = 100
    initial_state.save(sync.state_file)
    
    # Pull all instrumental files with resume
    state = sync.sync_from_remote(
        mock_webdav,
        components=["instrumental"],
        resume=True
    )
    
    # Verify only non-synced file was downloaded
    downloaded = set(mock_webdav._downloaded_files.keys())
    assert len(downloaded) == 1
    assert "track2_instrumental.mp3" in str(downloaded)
    
    # Verify state was preserved and updated
    assert "Artist1/Album1/track1_instrumental.mp3" in state.synced_files
    assert state.completed_bytes > initial_state.completed_bytes

def test_sync_error_handling(destination_dataset, mock_webdav):
    """Test handling of sync errors.
    
    This test verifies that:
    1. Errors during sync are properly caught and recorded
    2. Sync continues after file errors
    3. Failed files are tracked in sync state
    4. Successfully synced files are still recorded
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            }
        }
    }
    
    # Setup mock remote with files
    remote_files = [
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track2_instrumental.mp3"
    ]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Make one file fail to download
    def mock_download_with_error(remote_path, local_path):
        if remote_path == ".blackbird/schema.json":
            # Write remote schema
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'w') as f:
                json.dump(mock_webdav._remote_schema, f)
        elif "track1" in remote_path:
            raise Exception("Simulated download error")
        else:
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).touch()
            mock_webdav._downloaded_files[remote_path] = local_path
            
    mock_webdav.download_sync = mock_download_with_error
    
    # Try to pull all instrumental files
    state = sync.sync_from_remote(
        mock_webdav,
        components=["instrumental"]
    )
    
    # Verify error handling
    assert len(state.failed_files) == 1
    assert "track1" in next(iter(state.failed_files.keys()))
    assert "Simulated download error" in next(iter(state.failed_files.values()))
    
    # Verify successful files were still synced
    assert len(state.synced_files) == 1
    assert "track2" in next(iter(state.synced_files))

def test_exclude_patterns(destination_dataset, mock_webdav):
    """Test that exclude patterns are respected.
    
    This test verifies that:
    1. Files matching exclude patterns are not downloaded
    2. Other files are downloaded normally
    """
    sync = DatasetSync(destination_dataset)
    
    # Setup mock remote schema
    mock_webdav._remote_schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            }
        },
        "sync": {
            "exclude_patterns": ["*_temp.*"]
        }
    }
    
    # Add exclude pattern to schema
    sync.schema.schema["sync"]["exclude_patterns"] = ["*_temp.*"]
    sync.schema.save()
    
    # Setup mock remote with some files to exclude
    remote_files = [
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track1_instrumental_temp.mp3"
    ]
    for f in remote_files:
        mock_webdav._remote_files.add(f)
    
    # Pull instrumental files
    state = sync.sync_from_remote(
        mock_webdav,
        components=["instrumental"]
    )
    
    # Verify only non-excluded files were downloaded
    downloaded = set(mock_webdav._downloaded_files.keys())
    assert len(downloaded) == 1
    assert not any("_temp" in f for f in downloaded)
    assert all(not f.endswith("_temp.mp3") for f in state.synced_files) 