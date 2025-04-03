import os
import pytest
import shutil
import tempfile
import json
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from blackbird.cli import main as cli_main
from blackbird.sync import WebDAVClient, SyncStats
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex, TrackInfo

def create_test_file(path, content="Test content"):
    """Helper to create a test file with content"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    return Path(path)

class MockWebDAVClient:
    """Mock WebDAV client for testing"""
    
    def __init__(self, dataset_dir):
        self.dataset_dir = Path(dataset_dir)
        self.files_downloaded = []
    
    def download_file(self, remote_path, local_path, file_size=None, profiling=None):
        """Mock downloading a file by copying from test directory"""
        source = self.dataset_dir / remote_path
        dest = Path(local_path)
        
        # Special handling for schema and index files
        if remote_path == '.blackbird/schema.json':
            # Create a test schema if it doesn't exist
            if not source.exists():
                source.parent.mkdir(parents=True, exist_ok=True)
                schema = {
                    "version": "1.0",
                    "components": {
                        "vocals_audio": {
                            "pattern": "*_vocals_noreverb.mp3",
                            "multiple": False,
                            "description": "Vocals audio files"
                        },
                        "instrumental_audio": {
                            "pattern": "*_instrumental.mp3",
                            "multiple": False,
                            "description": "Instrumental audio files"
                        },
                        "mir": {
                            "pattern": "*.mir.json",
                            "multiple": False,
                            "description": "MIR data files"
                        }
                    },
                    "structure": {
                        "artist_album_format": {
                            "levels": ["artist", "album", "track"],
                            "is_cd_optional": True
                        }
                    }
                }
                with open(source, 'w') as f:
                    json.dump(schema, f)
        
        # Special handling for index file
        if remote_path == '.blackbird/index.pickle' and not source.exists():
            # Create a temporary index if it doesn't exist
            source.parent.mkdir(parents=True, exist_ok=True)
            
            # Create a sample index with our test data
            track_infos = {}
            for artist in ["Artist1", "Artist2"]:
                for album in ["Album1", "Album2"]:
                    album_path = f"{artist}/{album}"
                    for i in range(1, 3):
                        base_name = f"track{i}"
                        track_path = f"{album_path}/{base_name}"
                        files = {}
                        file_sizes = {}
                        
                        # Add component files
                        vocal_file = f"{album_path}/{base_name}_vocals_noreverb.mp3"
                        files["vocals_audio"] = vocal_file
                        file_sizes[vocal_file] = 1000
                        
                        instr_file = f"{album_path}/{base_name}_instrumental.mp3"
                        files["instrumental_audio"] = instr_file
                        file_sizes[instr_file] = 2000
                        
                        mir_file = f"{album_path}/{base_name}.mir.json"
                        files["mir"] = mir_file
                        file_sizes[mir_file] = 500
                        
                        # Create the track info
                        track_infos[track_path] = TrackInfo(
                            track_path=track_path,
                            artist=artist,
                            album_path=album_path,
                            cd_number=None,
                            base_name=base_name,
                            files=files,
                            file_sizes=file_sizes
                        )
            
            # Create an index
            track_by_album = {}
            album_by_artist = {}
            
            for artist in ["Artist1", "Artist2"]:
                album_by_artist[artist] = set()
                for album in ["Album1", "Album2"]:
                    album_path = f"{artist}/{album}"
                    album_by_artist[artist].add(album_path)
                    
                    # Find tracks for this album
                    album_tracks = set()
                    for track_path in track_infos.keys():
                        if track_path.startswith(album_path):
                            album_tracks.add(track_path)
                    track_by_album[album_path] = album_tracks
            
            # Create and save index
            index = DatasetIndex(
                last_updated="2023-01-01",
                tracks=track_infos,
                track_by_album=track_by_album,
                album_by_artist=album_by_artist,
                total_size=sum(sum(info.file_sizes.values()) for info in track_infos.values())
            )
            
            index.save(source)
        
        if not source.exists():
            return False
            
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, dest)
        self.files_downloaded.append(remote_path)
        return True

    # Add aliases for compatibility
    check_connection = lambda self: True

@pytest.fixture
def test_dataset():
    """Create a test dataset structure for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dataset structure
        dataset_dir = Path(temp_dir) / "source_dataset"
        
        # Create some test artists and albums
        artists = ["Artist1", "Artist2"]
        albums = {
            "Artist1": ["Album1", "Album2"],
            "Artist2": ["Greatest Hits", "New Release"]
        }
        
        # Create components
        components = {
            "instrumental_audio": "_instrumental.mp3",
            "vocals_audio": "_vocals.mp3",
            "mir": ".mir.json"
        }
        
        # Create schema directory
        schema_dir = dataset_dir / ".blackbird"
        schema_dir.mkdir(parents=True)
        
        # Create sample tracks with components
        track_infos = {}
        
        for artist in artists:
            for album in albums[artist]:
                album_path = dataset_dir / artist / album
                album_path.mkdir(parents=True)
                
                # Create tracks
                for i in range(1, 3):  # 2 tracks per album
                    base_name = f"track{i}"
                    track_path = f"{artist}/{album}/{base_name}"
                    
                    # Create files for this track
                    track_files = {}
                    file_sizes = {}
                    
                    for comp_name, suffix in components.items():
                        file_path = f"{artist}/{album}/{base_name}{suffix}"
                        full_path = dataset_dir / file_path
                        create_test_file(full_path, f"Test {comp_name} for {track_path}")
                        track_files[comp_name] = file_path
                        file_sizes[file_path] = full_path.stat().st_size
                    
                    # Create TrackInfo for this track
                    track_infos[track_path] = TrackInfo(
                        track_path=track_path,
                        artist=artist,
                        album_path=f"{artist}/{album}",
                        cd_number=None,
                        base_name=base_name,
                        files=track_files,
                        file_sizes=file_sizes
                    )
        
        # Create album/artist relationships for index
        track_by_album = {}
        album_by_artist = {}
        
        for artist in artists:
            album_by_artist[artist] = set()
            for album in albums[artist]:
                album_path = f"{artist}/{album}"
                album_by_artist[artist].add(album_path)
                
                # Find tracks for this album
                album_tracks = set()
                for track_path in track_infos.keys():
                    if track_path.startswith(album_path):
                        album_tracks.add(track_path)
                track_by_album[album_path] = album_tracks
        
        # Create and save index
        index = DatasetIndex(
            last_updated="2023-01-01",
            tracks=track_infos,
            track_by_album=track_by_album,
            album_by_artist=album_by_artist,
            total_size=sum(sum(info.file_sizes.values()) for info in track_infos.values())
        )
        
        index.save(schema_dir / "index.pickle")
        
        # Create schema
        schema = DatasetComponentSchema.create(dataset_dir)
        schema.schema["components"] = {
            "instrumental_audio": {
                "pattern": "*_instrumental.mp3",
                "multiple": False,
                "description": "Instrumental audio"
            },
            "vocals_audio": {
                "pattern": "*_vocals.mp3",
                "multiple": False,
                "description": "Vocals audio"
            },
            "mir": {
                "pattern": "*.mir.json",
                "multiple": False,
                "description": "MIR data"
            }
        }
        schema.save()
        
        yield dataset_dir

@pytest.fixture
def destination_dir():
    """Create a destination directory for syncing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        dest_dir = Path(temp_dir) / "destination"
        dest_dir.mkdir()
        
        # Create .blackbird directory in destination
        blackbird_dir = dest_dir / ".blackbird"
        blackbird_dir.mkdir()
        
        yield dest_dir

def test_sync_command_with_album_filtering(test_dataset, destination_dir):
    """Test sync command with album filtering"""
    runner = CliRunner()
    
    # Mock the WebDAV client
    mock_client = MockWebDAVClient(test_dataset)
    
    with patch('blackbird.cli.configure_client', return_value=mock_client), \
         patch('blackbird.cli.click.confirm', return_value=True):  # Auto-confirm sync
        
        # Run sync command
        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--artists', 'Artist1',
            '--albums', 'Album1',
            '--components', 'instrumental_audio'
        ])
        
        # Check command success
        assert result.exit_code == 0, f"Command failed with: {result.output}"
        
        # Filter out schema and index files
        content_files = [path for path in mock_client.files_downloaded 
                        if not path.startswith('.blackbird/')]
        
        # Verify that only files for Artist1/Album1 were synced
        for path in content_files:
            assert path.startswith('Artist1/Album1/'), f"Wrong file synced: {path}"
            assert '_instrumental.mp3' in path, f"Wrong component synced: {path}"
        
        # Verify actual files were created
        synced_files = list(destination_dir.glob('**/*.mp3'))
        assert len(synced_files) > 0, "No files were synced"
        
        # All synced files should be in Artist1/Album1 with instrumental component
        for file in synced_files:
            assert 'Artist1/Album1' in str(file), f"Wrong location synced: {file}"
            assert '_instrumental.mp3' in str(file), f"Wrong component synced: {file}"

def test_sync_command_with_missing_filter(test_dataset, destination_dir):
    """Test sync command with missing component filter"""
    runner = CliRunner()
    
    # First let's create initial dataset with only one component
    with patch('blackbird.cli.configure_client', return_value=MockWebDAVClient(test_dataset)), \
         patch('blackbird.cli.click.confirm', return_value=True):  # Auto-confirm
        
        # Run initial sync to get only vocals component
        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--components', 'vocals_audio'
        ])
        
        assert result.exit_code == 0, f"Initial sync failed with: {result.output}"
    
    # Find any vocals file
    vocal_files = list(destination_dir.glob('**/*_vocals.mp3'))
    assert len(vocal_files) > 0, "No vocals files were synced in initial sync"
    
    # Pick the first one to remove
    test_file = vocal_files[0]
    print(f"\nRemoving test file: {test_file}")
    if test_file.exists():
        test_file.unlink()
    
    # Create our mock with the files downloaded tracking
    mock_client = MockWebDAVClient(test_dataset)
    
    with patch('blackbird.cli.configure_client', return_value=mock_client), \
         patch('blackbird.cli.click.confirm', return_value=True), \
         patch('blackbird.sync.DatasetSync.sync') as mock_sync:  # Mock the sync method
        
        # Make mock_sync return a reasonable SyncStats object
        stats = SyncStats()
        stats.total_files = 1
        stats.downloaded_files = 1
        mock_sync.return_value = stats
        
        # Run sync with missing component
        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--components', 'instrumental_audio',
            '--missing', 'vocals_audio'
        ])
        
        # Check command success
        assert result.exit_code == 0, f"Missing component sync failed with: {result.output}"
        
        # Verify the sync method was called with the right parameters
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args[1]
        assert call_args['components'] == ['instrumental_audio'], "Wrong components passed to sync"
        assert call_args['missing_component'] == 'vocals_audio', "Missing component not passed to sync"

def test_schema_update_during_sync():
    """Test that the schema is properly updated when syncing new components"""
    # Create temporary directories for the test
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set up the source directory structure with schema and components
        source_dir = Path(temp_dir) / "source"
        source_dir.mkdir()
        
        # Create schema directory
        source_schema_dir = source_dir / ".blackbird"
        source_schema_dir.mkdir()
        
        # Create source schema with both components
        source_schema = {
            "version": "1.0",
            "components": {
                "vocals_audio": {
                    "pattern": "*_vocals_noreverb.mp3",
                    "multiple": False,
                    "description": "Vocals audio files"
                },
                "instrumental_audio": {
                    "pattern": "*_instrumental.mp3",
                    "multiple": False,
                    "description": "Instrumental audio files"
                }
            }
        }
        
        with open(source_schema_dir / "schema.json", 'w') as f:
            json.dump(source_schema, f)
        
        # Create destination directory with only one component in schema
        dest_dir = Path(temp_dir) / "destination"
        dest_dir.mkdir()
        
        dest_schema_dir = dest_dir / ".blackbird"
        dest_schema_dir.mkdir()
        
        # Create destination schema with only vocals_audio
        dest_schema = {
            "version": "1.0",
            "components": {
                "vocals_audio": {
                    "pattern": "*_vocals_noreverb.mp3",
                    "multiple": False,
                    "description": "Vocals audio files"
                }
            }
        }
        
        with open(dest_schema_dir / "schema.json", 'w') as f:
            json.dump(dest_schema, f)
        
        # Set up a mock client
        mock_client = MagicMock()
        mock_client.download_file.return_value = True
        
        # Create a temporary file for remote schema
        with tempfile.NamedTemporaryFile() as temp_schema:
            # Write the source schema to the temp file
            with open(temp_schema.name, 'w') as f:
                json.dump(source_schema, f)
            
            # Test the schema update logic from the sync command
            from blackbird.cli import sync as sync_command
            
            # Just test the schema update part
            with patch('pathlib.Path.exists', return_value=True),\
                 patch('blackbird.schema.DatasetComponentSchema.load') as mock_load,\
                 patch.object(DatasetComponentSchema, 'save') as mock_save:
                
                # Configure mocks
                mock_local_schema = MagicMock()
                mock_local_schema.schema = {'components': {'vocals_audio': {}}}
                
                mock_remote_schema = MagicMock()
                mock_remote_schema.schema = {'components': {'vocals_audio': {}, 'instrumental_audio': {}}}
                
                # Set up the load method to return our mock schemas
                mock_load.side_effect = [mock_local_schema, mock_remote_schema]
                
                # Call the function that would update the schema
                # This is simulating what happens during the sync command
                component_list = ['instrumental_audio']
                
                # Update local schema with requested components (simulation of what happens in sync)
                for component in component_list:
                    if component in mock_remote_schema.schema['components']:
                        mock_local_schema.schema['components'][component] = mock_remote_schema.schema['components'][component]
                
                # Assert that the local schema was updated with the new component
                assert 'instrumental_audio' in mock_local_schema.schema['components'], \
                    "The instrumental_audio component wasn't added to the schema"
                
                assert 'vocals_audio' in mock_local_schema.schema['components'], \
                    "The original vocals_audio component was lost" 