# blackbird/tests/test_cli_sync_locations.py
import pytest
from click.testing import CliRunner
from pathlib import Path
import os
import json
from unittest.mock import patch, MagicMock

# Assuming your CLI entry point is defined in blackbird.cli
from blackbird.cli import main as cli_main
from blackbird.locations import LocationsManager
from blackbird.index import DatasetIndex, TrackInfo # Assuming these classes exist
from blackbird.schema import DatasetComponentSchema # Assuming this class exists
from datetime import datetime

# Fixture for a temporary directory representing the dataset root
@pytest.fixture
def temp_dataset_root(tmp_path):
    dataset_root = tmp_path / "test_dataset"
    dataset_root.mkdir()
    (dataset_root / ".blackbird").mkdir()
    # Create default locations file pointing to Main location within the temp root
    locations_path = dataset_root / ".blackbird" / LocationsManager.LOCATIONS_FILENAME
    main_loc_path = dataset_root / "storage_main"
    main_loc_path.mkdir()
    with open(locations_path, 'w') as f:
        json.dump({LocationsManager.DEFAULT_LOCATION_NAME: str(main_loc_path)}, f)

    # Create a dummy schema and index
    schema_path = dataset_root / ".blackbird" / "schema.json"
    schema_data = {"version": "1.0", "components": {
        "vocals": {"pattern": "*_vocals.mp3", "multiple": False, "description": "Vocals"},
        "instr": {"pattern": "*_instr.mp3", "multiple": False, "description": "Instrumental"}
    }}
    with open(schema_path, 'w') as f:
        json.dump(schema_data, f)

    index_path = dataset_root / ".blackbird" / "index.pickle"
    # Create a simple index with one track, one file in 'Main' location conceptually
    track_info = TrackInfo(
        track_path="Main/Artist1/Album1/Track1",
        artist="Artist1",
        album_path="Main/Artist1/Album1",
        cd_number=None,
        base_name="Track1",
        files={"vocals": "Main/Artist1/Album1/Track1_vocals.mp3"},
        file_sizes={"Main/Artist1/Album1/Track1_vocals.mp3": 1024}
    )
    index = DatasetIndex(
        last_updated=datetime.now(),
        tracks={"Main/Artist1/Album1/Track1": track_info},
        track_by_album={"Main/Artist1/Album1": {"Main/Artist1/Album1/Track1"}},
        album_by_artist={"Artist1": {"Main/Artist1/Album1"}},
        total_size=1024,
        stats_by_location={"Main": {"file_count": 1, "total_size": 1024, "track_count": 1, "album_count": 1, "artist_count": 1}}
    )
    index.save(index_path)

    yield dataset_root # Provide the root path of the temporary dataset

    # Cleanup is handled by tmp_path fixture


# Fixture to add a secondary location
@pytest.fixture
def temp_dataset_with_second_location(temp_dataset_root):
    dataset_root = temp_dataset_root
    locations_path = dataset_root / ".blackbird" / LocationsManager.LOCATIONS_FILENAME
    second_loc_path = dataset_root / "storage_ssd"
    second_loc_path.mkdir()

    with open(locations_path, 'r') as f:
        locations = json.load(f)
    locations["SSD_Fast"] = str(second_loc_path)
    with open(locations_path, 'w') as f:
        json.dump(locations, f)

    return dataset_root, second_loc_path # Return root and the path to the second location


MOCK_SCHEMA_CONTENT = {"version": "1.0", "components": {
    "vocals": {"pattern": "*_vocals.mp3", "multiple": False},
    "instr": {"pattern": "*_instr.mp3", "multiple": False}
}}

MOCK_TRACK_INFO = TrackInfo(
    track_path="Main/Artist1/Album1/Track1", artist="Artist1",
    album_path="Main/Artist1/Album1", cd_number=None, base_name="Track1",
    files={"vocals": "Main/Artist1/Album1/Track1_vocals.mp3"},
    file_sizes={"Main/Artist1/Album1/Track1_vocals.mp3": 1024}
)


def _make_download_side_effect():
    """Shared download mock: handles schema, index, and audio file requests."""
    def download_side_effect(remote_path, local_path, file_size=None, **kwargs):
        if remote_path == ".blackbird/schema.json":
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'w') as f:
                json.dump(MOCK_SCHEMA_CONTENT, f)
            return True
        elif remote_path == ".blackbird/index.pickle":
            track_info = TrackInfo(
                track_path="Artist1/Album1/Track1", artist="Artist1",
                album_path="Artist1/Album1", cd_number=None, base_name="Track1",
                files={"vocals": "Artist1/Album1/Track1_vocals.mp3"},
                file_sizes={"Artist1/Album1/Track1_vocals.mp3": 1024}
            )
            index = DatasetIndex(
                last_updated=datetime.now(),
                tracks={"Artist1/Album1/Track1": track_info},
                track_by_album={"Artist1/Album1": {"Artist1/Album1/Track1"}},
                album_by_artist={"Artist1": {"Artist1/Album1"}}, total_size=1024,
                stats_by_location={}
            )
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            index.save(local_path)
            return True
        elif remote_path == "Artist1/Album1/Track1_vocals.mp3":
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(b'fake_vocals_data')
            os.truncate(local_path, 1024)
            return True
        else:
            return False
    return download_side_effect


def _build_mock_client():
    """Create a MagicMock WebDAV client with schema, index, and download mocks."""
    mock = MagicMock()
    mock.check_connection.return_value = True
    mock.base_url = "http://fake-server"
    mock.client = MagicMock()
    mock.client.options = {'webdav_root': '/dataset/'}

    mock_schema_object = MagicMock(spec=DatasetComponentSchema)
    mock_schema_object.schema = MOCK_SCHEMA_CONTENT
    mock.get_schema = MagicMock(return_value=mock_schema_object)

    mock_index = DatasetIndex(
        last_updated=datetime.now(),
        tracks={"Main/Artist1/Album1/Track1": MOCK_TRACK_INFO},
        track_by_album={"Main/Artist1/Album1": {"Main/Artist1/Album1/Track1"}},
        album_by_artist={"Artist1": {"Main/Artist1/Album1"}}, total_size=1024,
        stats_by_location={}
    )
    mock.get_index = MagicMock(return_value=mock_index)
    mock.download_file.side_effect = _make_download_side_effect()
    return mock


@pytest.fixture
def mock_webdav_for_sync():
    """Mock WebDAV client for sync command tests (patches blackbird.cli)."""
    with patch('blackbird.cli.configure_client') as mock_configure:
        client = _build_mock_client()
        mock_configure.return_value = client
        yield client


@pytest.fixture
def mock_webdav_for_clone():
    """Mock WebDAV client for clone command tests (patches blackbird.sync)."""
    with patch('blackbird.sync.configure_client') as mock_configure:
        client = _build_mock_client()
        mock_configure.return_value = client
        yield client


# Test clone to default location
def test_clone_to_default_location(tmp_path, mock_webdav_for_clone):
    runner = CliRunner()
    local_clone_path = tmp_path / "local_clone"
    result = runner.invoke(cli_main, [
        'clone',
        'webdav://fake-server/dataset',
        str(local_clone_path),
        '--components', 'vocals'
        # No --target-location specified, should default to Main
    ])

    print("Clone Output:\n", result.output) # Print output for debugging
    assert result.exit_code == 0, f"CLI exited with code {result.exit_code}: {result.output}"

    # Verify locations file was created with Main pointing inside the clone dir
    locations_file = local_clone_path / ".blackbird" / "locations.json"
    assert locations_file.exists()
    with open(locations_file, 'r') as f:
        locations = json.load(f)
    assert LocationsManager.DEFAULT_LOCATION_NAME in locations
    main_storage_path = Path(locations[LocationsManager.DEFAULT_LOCATION_NAME])
    assert main_storage_path.exists()
    assert main_storage_path.is_dir()
    assert main_storage_path == local_clone_path # New assertion: default Main IS the clone root
    
    # Also verify the actual downloaded file exists within the Main location (which is the root)
    expected_file_path = local_clone_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    assert expected_file_path.exists(), f"File not found at {expected_file_path}"


# Test clone to specific location
def test_clone_to_specific_location(tmp_path, mock_webdav_for_clone):
    runner = CliRunner()
    local_clone_path = tmp_path / "local_clone_specific"
    # We need to pre-create the locations file and the target directory *before* cloning
    # because clone_dataset expects the target location to exist.
    local_clone_path.mkdir()
    (local_clone_path / ".blackbird").mkdir()
    locations_path = local_clone_path / ".blackbird" / LocationsManager.LOCATIONS_FILENAME
    main_loc_path = local_clone_path / "storage_main"
    main_loc_path.mkdir()
    ssd_loc_path = local_clone_path / "storage_ssd" # Create the target dir
    ssd_loc_path.mkdir()
    locations_data = {
        LocationsManager.DEFAULT_LOCATION_NAME: str(main_loc_path),
        "SSD_Fast": str(ssd_loc_path)
    }
    with open(locations_path, 'w') as f:
        json.dump(locations_data, f)


    result = runner.invoke(cli_main, [
        'clone',
        'webdav://fake-server/dataset',
        str(local_clone_path),
        '--components', 'vocals',
        '--target-location', 'SSD_Fast' # Specify target location
    ])

    print("Clone Specific Output:\n", result.output)
    assert result.exit_code == 0, f"CLI exited with code {result.exit_code}: {result.output}"

    # Verify the file landed in the correct *absolute* path within the "SSD_Fast" location
    expected_file_path = ssd_loc_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    assert expected_file_path.exists(), f"File not found at {expected_file_path}"
    assert expected_file_path.stat().st_size == 1024

    # Verify the "Main" location is empty
    main_file_path = main_loc_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    assert not main_file_path.exists()


# Test sync to specific location
def test_sync_to_specific_location(temp_dataset_with_second_location, mock_webdav_for_sync):
    runner = CliRunner()
    dataset_root, second_loc_path = temp_dataset_with_second_location

    # Ensure the file doesn't exist in the target location initially
    target_file_path = second_loc_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    assert not target_file_path.exists()

    result = runner.invoke(cli_main, [
        'sync',
        'webdav://fake-server/dataset', # Source URL
        str(dataset_root),              # Destination Root Path
        '--components', 'vocals',
        '--target-location', 'SSD_Fast' # Sync to the second location
    ])

    print("Sync Specific Output:\n", result.output)
    assert result.exit_code == 0, f"CLI exited with code {result.exit_code}: {result.output}"

    # Verify the file now exists in the target location "SSD_Fast"
    assert target_file_path.exists()
    assert target_file_path.stat().st_size == 1024

    # Verify the "Main" location storage is still empty for this file
    main_loc_path = Path(json.load(open(dataset_root / ".blackbird" / "locations.json"))["Main"])
    main_file_path = main_loc_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    assert not main_file_path.exists()


# Test sync correctly skips existing files in the target location
def test_sync_skips_existing_in_target(temp_dataset_with_second_location, mock_webdav_for_sync):
    runner = CliRunner()
    dataset_root, second_loc_path = temp_dataset_with_second_location

    # Pre-create the file in the target location ("SSD_Fast")
    target_file_path = second_loc_path / "Artist1" / "Album1" / "Track1_vocals.mp3"
    target_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_file_path, 'wb') as f:
        f.write(b'existing_fake_data')
    os.truncate(target_file_path, 1024) # Ensure size matches mock index
    assert target_file_path.exists()

    # Reset the download count on the mock to check if it gets called
    mock_webdav_for_sync.download_file.reset_mock()

    result = runner.invoke(cli_main, [
        'sync',
        'webdav://fake-server/dataset',
        str(dataset_root),
        '--components', 'vocals',
        '--target-location', 'SSD_Fast'
    ])

    print("Sync Skip Output:\n", result.output)
    assert result.exit_code == 0, f"CLI exited with code {result.exit_code}: {result.output}"

    # Assert that the download function was NOT called because the file existed
    # mock_webdav_for_sync.download_file.assert_not_called()
    # We need to check the specific call wasn't made, as schema/index might be called
    download_calls = mock_webdav_for_sync.download_file.call_args_list
    print("Download calls:", download_calls)
    # Check if the specific file download was attempted
    file_download_attempted = any(
        call.kwargs.get('remote_path') == 'Artist1/Album1/Track1_vocals.mp3'
        for call in download_calls
    )
    assert not file_download_attempted, "Download was called for the existing file, but it should have been skipped."

    # Check the output for skipped file count (adjust based on actual CLI output format)
    # Look for "Skipped: 1" in the summary section
    assert "Skipped: 1" in result.output 