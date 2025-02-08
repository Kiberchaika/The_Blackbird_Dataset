import pytest
from pathlib import Path
import shutil
import json
from datetime import datetime
from unittest.mock import MagicMock, patch
from webdav3.client import Client
from blackbird.sync import DatasetSync, SyncState
from blackbird.schema import DatasetComponentSchema, SchemaValidationResult

@pytest.fixture
def mock_webdav():
    """Create a mock WebDAV client with schema support."""
    client = MagicMock(spec=Client)
    
    # Track remote schema, files, and directories
    client._remote_schema = {
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
            "mir": {  # New component not in local schema
                "pattern": "*.mir.json",
                "required": False,
                "description": "Music Information Retrieval data"
            }
        },
        "sync": {
            "default_components": ["instrumental", "vocals"],
            "exclude_patterns": ["*.tmp", "*.bak"]
        }
    }
    client._remote_files = {
        "Artist1/Album1/track1_instrumental.mp3": b"data1",
        "Artist1/Album1/track1_vocals_noreverb.mp3": b"data2",
        "Artist1/Album1/track1.mir.json": b"data3",
        "Artist1/Album1/track2_instrumental.mp3": b"data4",
        "Artist1/Album1/track2_vocals_noreverb.mp3": b"data5",
        "Artist1/Album1/track2.mir.json": b"data6"
    }
    client._remote_file_sizes = {
        "Artist1/Album1/track1_instrumental.mp3": 1000,
        "Artist1/Album1/track1_vocals_noreverb.mp3": 800,
        "Artist1/Album1/track1.mir.json": 200,
        "Artist1/Album1/track2_instrumental.mp3": 1200,
        "Artist1/Album1/track2_vocals_noreverb.mp3": 900,
        "Artist1/Album1/track2.mir.json": 250
    }
    
    def mock_download_sync(remote_path, local_path):
        if remote_path == ".blackbird/schema.json":
            with open(local_path, 'w') as f:
                json.dump(client._remote_schema, f)
        elif remote_path in client._remote_files:
            with open(local_path, 'wb') as f:
                f.write(client._remote_files[remote_path])
        else:
            raise ValueError(f"File not found: {remote_path}")
            
    def mock_list(pattern=None):
        if pattern:
            return [
                path for path in client._remote_files.keys()
                if Path(path).match(pattern)
            ]
        return list(client._remote_files.keys())
        
    def mock_info(path):
        if path in client._remote_file_sizes:
            return {"size": client._remote_file_sizes[path]}
        raise ValueError(f"File not found: {path}")
        
    client.download_sync = mock_download_sync
    client.list = mock_list
    client.info = mock_info
    
    return client

@pytest.fixture
def test_dataset(tmp_path):
    """Create a test dataset with basic schema."""
    dataset_root = tmp_path / "test_dataset"
    
    # Create schema with only instrumental component
    schema = DatasetComponentSchema.create(dataset_root)
    schema.schema["components"] = {
        "instrumental": {
            "pattern": "*_instrumental.mp3",
            "required": True
        }
    }
    schema.save()
    
    yield dataset_root
    
    # Cleanup
    shutil.rmtree(dataset_root)

def test_get_remote_schema(test_dataset, mock_webdav):
    """Test fetching and parsing remote schema."""
    sync = DatasetSync(test_dataset)
    remote_schema = sync._get_remote_schema(mock_webdav)
    
    assert remote_schema["version"] == "1.0"
    assert "mir" in remote_schema["components"]
    assert remote_schema["components"]["mir"]["pattern"] == "*.mir.json"

def test_merge_schema(test_dataset, mock_webdav):
    """Test merging remote schema with local schema."""
    sync = DatasetSync(test_dataset)
    remote_schema = sync._get_remote_schema(mock_webdav)
    result = sync._merge_schema(remote_schema)
    
    assert result.is_valid
    
    # Check that new component was added
    assert "mir" in sync.schema.schema["components"]
    assert sync.schema.schema["components"]["mir"]["pattern"] == "*.mir.json"
    
    # Check that existing component was preserved
    assert sync.schema.schema["components"]["instrumental"]["required"] is True

def test_schema_version_mismatch(test_dataset, mock_webdav):
    """Test handling schema version mismatch."""
    sync = DatasetSync(test_dataset)
    
    # Modify remote schema version
    mock_webdav._remote_schema["version"] = "2.0"
    
    with pytest.raises(ValueError, match="Schema version mismatch"):
        remote_schema = sync._get_remote_schema(mock_webdav)
        sync._merge_schema(remote_schema)

def test_sync_with_schema_update(test_dataset, mock_webdav):
    """Test full sync process with schema update."""
    sync = DatasetSync(test_dataset)
    
    # Run sync with new component
    state = sync.sync(mock_webdav, components=["instrumental", "mir"])
    
    # Verify schema was updated
    assert "mir" in sync.schema.schema["components"]
    
    # Verify files were synced
    synced_files = {
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track1.mir.json",
        "Artist1/Album1/track2_instrumental.mp3",
        "Artist1/Album1/track2.mir.json"
    }
    assert state.synced_files == synced_files
    
    # Verify files exist on disk
    for file in synced_files:
        assert (test_dataset / file).exists()

def test_sync_resume_after_schema_update(test_dataset, mock_webdav):
    """Test resuming sync after schema update."""
    sync = DatasetSync(test_dataset)
    
    # Create initial sync state
    state = SyncState.create_new()
    state.synced_files.add("Artist1/Album1/track1_instrumental.mp3")
    state.completed_bytes = 1000
    state.save(sync.state_file)
    
    # Run sync with resume
    final_state = sync.sync(
        mock_webdav,
        components=["instrumental", "mir"],
        resume=True
    )
    
    # Verify previously synced file was preserved
    assert "Artist1/Album1/track1_instrumental.mp3" in final_state.synced_files
    
    # Verify new files were synced
    expected_new_files = {
        "Artist1/Album1/track1.mir.json",
        "Artist1/Album1/track2_instrumental.mp3",
        "Artist1/Album1/track2.mir.json"
    }
    assert all(f in final_state.synced_files for f in expected_new_files)
    
    # Verify completed bytes were accumulated
    assert final_state.completed_bytes > state.completed_bytes

def test_sync_with_invalid_component(test_dataset, mock_webdav):
    """Test sync with component not in schema."""
    sync = DatasetSync(test_dataset)
    
    with pytest.raises(ValueError, match="Invalid components"):
        sync.sync(mock_webdav, components=["nonexistent"])

def test_sync_progress_callback(test_dataset, mock_webdav):
    """Test progress callback during sync."""
    sync = DatasetSync(test_dataset)
    progress_messages = []
    
    def progress_callback(msg):
        progress_messages.append(msg)
    
    sync.sync(
        mock_webdav,
        components=["instrumental"],
        progress_callback=progress_callback
    )
    
    assert "Getting remote schema..." in progress_messages
    assert "Finding files to sync..." in progress_messages
    assert "Calculating total size..." in progress_messages
    assert any(msg.startswith("Syncing ") for msg in progress_messages)

def test_sync_state_save_load(test_dataset):
    """Test saving and loading sync state."""
    state = SyncState.create_new()
    state.synced_files.add("test1.mp3")
    state.failed_files["test2.mp3"] = "Failed to upload"
    state.total_bytes = 1000
    state.completed_bytes = 500
    
    state_file = test_dataset / ".blackbird" / "sync_state.json"
    state.save(state_file)
    
    loaded_state = SyncState.load(state_file)
    assert loaded_state.synced_files == {"test1.mp3"}
    assert loaded_state.failed_files == {"test2.mp3": "Failed to upload"}
    assert loaded_state.total_bytes == 1000
    assert loaded_state.completed_bytes == 500

def test_sync_all_components(test_dataset, mock_webdav):
    """Test syncing all components."""
    sync = DatasetSync(test_dataset)
    
    # Run sync
    state = sync.sync(mock_webdav, components=["instrumental", "vocals", "lyrics"])
    
    # Check created directories
    assert "Artist1" in mock_webdav._created_dirs
    assert "Artist1/Album1" in mock_webdav._created_dirs
    
    # Check uploaded files
    uploaded_files = set(mock_webdav._uploaded_files.keys())
    expected_files = {
        "Artist1/Album1/track1_instrumental.mp3",
        "Artist1/Album1/track1_vocals_noreverb.mp3",
        "Artist1/Album1/track1_vocals_noreverb.json",
        "Artist1/Album1/track2_instrumental.mp3",
        "Artist1/Album1/track2_vocals_noreverb.mp3"
    }
    assert uploaded_files == expected_files
    
    # Check sync state
    assert len(state.synced_files) == 5
    assert not state.failed_files
    assert state.total_bytes > 0
    assert state.completed_bytes == state.total_bytes

def test_sync_selective_components(test_dataset, mock_webdav):
    """Test syncing specific components."""
    sync = DatasetSync(test_dataset)
    
    # Sync only vocals
    state = sync.sync(mock_webdav, components=["vocals"])
    
    # Check uploaded files
    uploaded_files = set(mock_webdav._uploaded_files.keys())
    expected_files = {
        "Artist1/Album1/track1_vocals_noreverb.mp3",
        "Artist1/Album1/track2_vocals_noreverb.mp3"
    }
    assert uploaded_files == expected_files

def test_sync_resume(test_dataset, mock_webdav):
    """Test resuming sync operation."""
    sync = DatasetSync(test_dataset)
    
    # Create existing sync state
    state = SyncState.create_new()
    state.synced_files.add("Artist1/Album1/track1_instrumental.mp3")
    state.completed_bytes = 100
    state.save(sync.state_file)
    
    # Run sync
    final_state = sync.sync(mock_webdav, components=["instrumental"])
    
    # Check that previously synced file was skipped
    uploaded_files = set(mock_webdav._uploaded_files.keys())
    assert "Artist1/Album1/track1_instrumental.mp3" not in uploaded_files
    assert "Artist1/Album1/track2_instrumental.mp3" in uploaded_files
    
    # Check state was preserved
    assert "Artist1/Album1/track1_instrumental.mp3" in final_state.synced_files
    assert final_state.completed_bytes > state.completed_bytes

def test_sync_exclude_patterns(test_dataset, mock_webdav):
    """Test exclusion patterns during sync."""
    sync = DatasetSync(test_dataset)
    
    # Run sync
    state = sync.sync(mock_webdav)
    
    # Check that temporary files were excluded
    uploaded_files = set(mock_webdav._uploaded_files.keys())
    assert not any(f.endswith('.tmp') for f in uploaded_files)
    assert not any(f.endswith('.bak') for f in uploaded_files) 