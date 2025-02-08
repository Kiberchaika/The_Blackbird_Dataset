import pytest
from pathlib import Path
import shutil
import tempfile
import json
from webdav3.client import Client
from blackbird.sync import DatasetSync
from blackbird.schema import DatasetComponentSchema

@pytest.fixture(scope="session")
def webdav_test_env():
    """Set up test environment on WebDAV server.
    
    This fixture:
    1. Creates remote schema
    2. Creates test files
    3. Cleans up after tests
    """
    # Configure WebDAV client
    client = Client({
        'webdav_hostname': "http://localhost:8080",
        'webdav_login': "user",
        'webdav_password': "test123"
    })
    
    # Create remote schema
    schema = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True,
                "description": "Instrumental tracks"
            },
            "vocals": {
                "pattern": "*_vocals_noreverb.mp3",
                "required": False,
                "description": "Isolated vocals without reverb"
            }
        },
        "sync": {
            "default_components": ["instrumental"],
            "exclude_patterns": ["*.tmp", "*.bak"]
        }
    }
    
    # Create temporary file for schema
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
        json.dump(schema, f, indent=2)
        f.flush()
        
        # Create .blackbird directory
        try:
            client.mkdir(".blackbird")
        except:
            pass  # Directory might already exist
            
        # Upload schema
        client.upload_sync(
            remote_path=".blackbird/schema.json",
            local_path=f.name
        )
    
    yield client
    
    # Cleanup
    try:
        client.clean(".blackbird")
    except:
        pass  # Ignore cleanup errors

@pytest.mark.live
def test_live_sync(webdav_test_env):
    """Test syncing from a live WebDAV server."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "test_dataset"
        
        try:
            # Create dataset with schema
            schema = DatasetComponentSchema.create(dataset_path)
            schema.schema["components"] = {
                "instrumental": {
                    "pattern": "*_instrumental.mp3",
                    "required": True,
                    "description": "Instrumental tracks"
                }
            }
            schema.save()
            
            # Configure sync
            sync = DatasetSync(dataset_path)
            client = sync.configure_client(
                webdav_url="http://localhost:8080",
                username="user",
                password="test123"
            )
            
            # Run sync
            state = sync.sync(client, components=["instrumental"])
            
            # Verify schema was updated from remote
            assert len(sync.schema.schema["components"]) >= 1
            assert "vocals" in sync.schema.schema["components"]
            
            # Verify files were synced
            assert len(state.synced_files) > 0
            assert not state.failed_files
            
            # Verify file structure
            synced_file = next(iter(state.synced_files))
            assert (dataset_path / synced_file).exists()
            
        finally:
            # Clean up
            if dataset_path.exists():
                shutil.rmtree(dataset_path)

@pytest.mark.live
def test_live_sync_selective(webdav_test_env):
    """Test selective component sync from live server."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "test_dataset"
        
        try:
            # Create dataset with schema
            schema = DatasetComponentSchema.create(dataset_path)
            schema.schema["components"] = {
                "instrumental": {
                    "pattern": "*_instrumental.mp3",
                    "required": True,
                    "description": "Instrumental tracks"
                },
                "vocals": {
                    "pattern": "*_vocals_noreverb.mp3",
                    "required": False,
                    "description": "Isolated vocals without reverb"
                }
            }
            schema.save()
            
            # Configure sync
            sync = DatasetSync(dataset_path)
            client = sync.configure_client(
                webdav_url="http://localhost:8080",
                username="user",
                password="test123"
            )
            
            # Sync only vocals
            state = sync.sync(client, components=["vocals"])
            
            # Verify only vocals were synced
            assert all("vocals_noreverb" in f for f in state.synced_files)
            assert not any("instrumental" in f for f in state.synced_files)
            
        finally:
            # Clean up
            if dataset_path.exists():
                shutil.rmtree(dataset_path)

@pytest.mark.live
def test_live_sync_resume(webdav_test_env):
    """Test resuming sync from live server."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "test_dataset"
        
        try:
            # Create dataset with schema
            schema = DatasetComponentSchema.create(dataset_path)
            schema.schema["components"] = {
                "instrumental": {
                    "pattern": "*_instrumental.mp3",
                    "required": True,
                    "description": "Instrumental tracks"
                }
            }
            schema.save()
            
            # Configure sync
            sync = DatasetSync(dataset_path)
            client = sync.configure_client(
                webdav_url="http://localhost:8080",
                username="user",
                password="test123"
            )
            
            # Run initial sync
            initial_state = sync.sync(client, components=["instrumental"])
            
            # Modify sync state to simulate partial sync
            initial_state.synced_files = set(list(initial_state.synced_files)[:1])
            initial_state.save(sync.state_file)
            
            # Run resume sync
            final_state = sync.sync(client, components=["instrumental"], resume=True)
            
            # Verify files were properly resumed
            assert len(final_state.synced_files) > len(initial_state.synced_files)
            assert all(f in final_state.synced_files for f in initial_state.synced_files)
            
        finally:
            # Clean up
            if dataset_path.exists():
                shutil.rmtree(dataset_path) 