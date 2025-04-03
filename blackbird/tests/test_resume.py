import pytest
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from unittest.mock import patch, MagicMock, call
from dataclasses import dataclass, field
from datetime import datetime

import pickle # Needed for index creation

from click.testing import CliRunner

# Assume these imports exist based on the project structure
from blackbird.operations import (
    create_operation_state,
    load_operation_state,
    delete_operation_state,
    OperationState,
    OperationStatus,
    find_latest_state_file,
    get_state_file_path
)
from blackbird.sync import resume_sync_operation, SyncState, WebDAVClient, SyncStats
from blackbird.index import DatasetIndex, TrackInfo # Assuming TrackInfo is defined here
from blackbird.locations import LocationsManager, resolve_symbolic_path, SymbolicPathError # Added SymbolicPathError
from blackbird.dataset import Dataset
from blackbird.cli import main # Import the main CLI entry point

# --- Fixtures ---

@pytest.fixture
def temp_dataset(tmp_path: Path) -> Path:
    """Creates a temporary dataset directory structure."""
    ds_path = tmp_path / "test_dataset"
    bb_dir = ds_path / ".blackbird"
    bb_dir.mkdir(parents=True)
    return ds_path

@pytest.fixture
def sample_locations(temp_dataset: Path) -> Dict[str, Path]:
    """Creates a sample locations.json and returns the location dict."""
    locations_file = temp_dataset / ".blackbird" / "locations.json"
    locations_data = {
        "Main": str(temp_dataset / "storage_main"),
        "SSD": str(temp_dataset / "storage_ssd")
    }
    # Create the directories themselves
    Path(locations_data["Main"]).mkdir(parents=True)
    Path(locations_data["SSD"]).mkdir(parents=True)
    with open(locations_file, 'w') as f:
        json.dump(locations_data, f)
    # Return paths as Path objects
    return {name: Path(path) for name, path in locations_data.items()}


@pytest.fixture
def sample_index_data() -> Dict[int, Tuple[str, int]]:
    """Provides sample file_info_by_hash data."""
    # hash(symbolic_path) -> (symbolic_path, size)
    return {
        hash("Main/ArtistA/Album1/track1_vocals.mp3"): ("Main/ArtistA/Album1/track1_vocals.mp3", 1024),
        hash("Main/ArtistA/Album1/track1.mir.json"): ("Main/ArtistA/Album1/track1.mir.json", 512),
        hash("SSD/ArtistB/Album2/track2_instr.mp3"): ("SSD/ArtistB/Album2/track2_instr.mp3", 2048),
        hash("SSD/ArtistB/Album2/track2_lyrics.txt"): ("SSD/ArtistB/Album2/track2_lyrics.txt", 256),
        hash("Main/ArtistA/Album1/track_already_exists.mp3"): ("Main/ArtistA/Album1/track_already_exists.mp3", 3000), # For testing existing files
         hash("Main/ArtistA/Album1/track_size_mismatch.mp3"): ("Main/ArtistA/Album1/track_size_mismatch.mp3", 4000), # For testing size mismatch
    }

@pytest.fixture
def sample_index_file(temp_dataset: Path, sample_index_data: Dict[int, Tuple[str, int]]):
    """Creates a sample index.pickle file."""
    index_path = temp_dataset / ".blackbird" / "index.pickle"
    # Create a minimal index structure
    index = DatasetIndex(
        last_updated=datetime.now(),
        tracks={}, # Keep simple for resume test focus
        track_by_album={},
        album_by_artist={},
        total_size=sum(size for _, size in sample_index_data.values()),
        stats_by_location={},
        file_info_by_hash=sample_index_data,
        version="1.0"
    )
    with open(index_path, 'wb') as f:
        pickle.dump(index, f, protocol=5)
    return index_path

@pytest.fixture
def sample_state_file(temp_dataset: Path, sample_index_data: Dict[int, Tuple[str, int]]) -> Path:
    """Creates a sample operation state file for sync."""
    bb_dir = temp_dataset / ".blackbird"
    source_url = "webdav://mockserver/remote_dataset"
    target_loc = "Main" # Target location for the sync operation

    file_hashes = list(sample_index_data.keys())
    # Mark some as done, some as failed, some as pending
    hashes_status: Dict[int, OperationStatus] = {}
    hashes_status[file_hashes[0]] = "pending" # track1_vocals.mp3
    hashes_status[file_hashes[1]] = "done" # track1.mir.json
    hashes_status[file_hashes[2]] = "failed: Network error" # track2_instr.mp3
    hashes_status[file_hashes[3]] = "pending" # track2_lyrics.txt
    hashes_status[file_hashes[4]] = "pending" # track_already_exists.mp3
    hashes_status[file_hashes[5]] = "pending" # track_size_mismatch.mp3


    state_data: OperationState = {
        "operation_type": "sync",
        "timestamp": time.time(),
        "source": source_url,
        "target_location": target_loc,
        "components": ["vocals", "mir", "instr", "lyrics"], # Example components
        "files": hashes_status,
    }

    state_file_path = get_state_file_path(bb_dir, state_data['operation_type'], state_data['timestamp'])

    # Save the state file
    with open(state_file_path, "w") as f:
        # Convert int keys back to str for JSON
        state_to_save = state_data.copy()
        state_to_save['files'] = {str(k): v for k, v in state_data['files'].items()}
        json.dump(state_to_save, f, indent=2)

    return state_file_path

# --- Test Cases ---

def test_state_file_creation_and_deletion(temp_dataset: Path):
    """Verify creation and deletion of state files."""
    bb_dir = temp_dataset / ".blackbird"
    hashes = [123, 456, 789]
    op_type = "sync"
    source = "webdav://test"
    target_loc = "Main"

    # Test creation
    state_file_path = create_operation_state(bb_dir, op_type, source, target_loc, hashes)
    assert state_file_path.exists()
    assert state_file_path.name.startswith(f"operation_{op_type}_")
    assert state_file_path.name.endswith(".json")

    # Verify content (basic check)
    loaded_state = load_operation_state(state_file_path)
    assert loaded_state is not None
    assert loaded_state["operation_type"] == op_type
    assert loaded_state["source"] == source
    assert loaded_state["target_location"] == target_loc
    assert set(loaded_state["files"].keys()) == set(hashes)
    assert all(status == "pending" for status in loaded_state["files"].values())

    # Test deletion
    delete_operation_state(state_file_path)
    assert not state_file_path.exists()

    # Test finding latest (should be None after deletion)
    assert find_latest_state_file(bb_dir, op_type) is None


@patch('blackbird.sync.configure_client') # Mock the client configuration
@patch('blackbird.sync.DatasetSync._download_file') # Mock the actual download method
def test_resume_success(
    mock_download_file: MagicMock,
    mock_configure_client: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path,
    sample_index_data: Dict[int, Tuple[str, int]]
):
    """Test successful resume where pending/failed items are downloaded."""
    mock_client_instance = MagicMock(spec=WebDAVClient)
    mock_configure_client.return_value = mock_client_instance

    # Mock download success: returns (success_bool, downloaded_size)
    mock_download_file.return_value = (True, 100) # Assume all downloads succeed with 100 bytes

    # Define which files should exist *before* resume for skip test
    existing_hash = hash("Main/ArtistA/Album1/track_already_exists.mp3")
    existing_sym_path, existing_size = sample_index_data[existing_hash]
    
    # Define file with size mismatch
    mismatch_hash = hash("Main/ArtistA/Album1/track_size_mismatch.mp3")
    mismatch_sym_path, mismatch_expected_size = sample_index_data[mismatch_hash]


    # Resolve paths based on the *target* location from the state file ("Main")
    main_loc_path = sample_locations["Main"]
    existing_local_path = main_loc_path / Path(existing_sym_path).relative_to("Main")
    existing_local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(existing_local_path, "wb") as f:
        f.write(b'a' * existing_size) # Create file with correct size

    mismatch_local_path = main_loc_path / Path(mismatch_sym_path).relative_to("Main")
    mismatch_local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(mismatch_local_path, "wb") as f:
        f.write(b'b' * (mismatch_expected_size + 10)) # Create file with INCORRECT size
    
    original_state = load_operation_state(sample_state_file)
    assert original_state is not None

    # --- Run the resume function ---
    # Add patch for Path.unlink to track deletion attempts
    # Also patch delete_operation_state from sync module where it's called
    with patch('pathlib.Path.unlink') as mock_unlink, \
         patch('blackbird.sync.delete_operation_state') as mock_delete_state:
        success = resume_sync_operation(
            dataset_path=temp_dataset,
            state_file_path=sample_state_file,
            state=original_state,
            parallel=1 # Test sequentially first
        )

    # --- Assertions ---
    assert success is True
    # assert not sample_state_file.exists(), "State file should be deleted on success" # Check mock instead
    mock_delete_state.assert_called_once_with(sample_state_file)

    # Verify which files were attempted for download
    # Expected downloads:
    # - track1_vocals.mp3 (pending)
    # - track2_instr.mp3 (failed) -> will be retried (cross-location)
    # - track2_lyrics.txt (pending) (cross-location)
    # - track_size_mismatch.mp3 (pending, size mismatch) -> will be redownloaded
    # Expected skips:
    # - track1.mir.json (done)
    # - track_already_exists.mp3 (pending, but exists with correct size)
    
    expected_downloads_count = 4
    assert mock_download_file.call_count == expected_downloads_count

    # Verify the size-mismatched file was deleted before redownload attempt
    mock_unlink.assert_called_once_with(missing_ok=True)


    # Verify the file with size mismatch was redownloaded
    hash_mismatch = hash("Main/ArtistA/Album1/track_size_mismatch.mp3")
    sym_path_mismatch, size_mismatch = sample_index_data[hash_mismatch]
    # The remote path passed to download should be relative to the location root
    rel_path_mismatch = str(Path(sym_path_mismatch).relative_to("Main"))
    local_path_mismatch = sample_locations["Main"] / rel_path_mismatch
    found_mismatch_call = False
    for call_args in mock_download_file.call_args_list:
        kwargs = call_args.kwargs
        # Check remote_path and local_path in the call arguments
        if kwargs.get('remote_path') == rel_path_mismatch and kwargs.get('local_path') == local_path_mismatch:
             found_mismatch_call = True
             break
    assert found_mismatch_call, "Download should have been called for the size-mismatched file"


@patch('blackbird.sync.configure_client')
@patch('blackbird.sync.DatasetSync._download_file')
def test_resume_failure(
    mock_download_file: MagicMock,
    mock_configure_client: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path,
    sample_index_data: Dict[int, Tuple[str, int]]
):
    """Test resume where some downloads fail."""
    mock_client_instance = MagicMock(spec=WebDAVClient)
    mock_configure_client.return_value = mock_client_instance

    # Mock download failure for one specific file
    fail_hash = hash("Main/ArtistA/Album1/track1_vocals.mp3")
    fail_sym_path, fail_size = sample_index_data[fail_hash]
    fail_rel_path = str(Path(fail_sym_path).relative_to("Main"))

    def download_side_effect(*args, **kwargs):
        # Fail only for the specific file
        if kwargs.get('remote_path') == fail_rel_path:
            return (False, 0) # Failed download
        else:
            # Succeed for others (size doesn't matter much here)
            return (True, 100)

    mock_download_file.side_effect = download_side_effect

    original_state = load_operation_state(sample_state_file)
    assert original_state is not None

    # --- Run the resume function ---
    success = resume_sync_operation(
        dataset_path=temp_dataset,
        state_file_path=sample_state_file,
        state=original_state,
    )

    # --- Assertions ---
    assert success is False, "Resume should report failure if downloads fail"
    assert sample_state_file.exists(), "State file should NOT be deleted on failure"

    # Verify the state file reflects the failure
    final_state = load_operation_state(sample_state_file)
    assert final_state is not None
    assert fail_hash in final_state["files"]
    # The status should indicate failure
    assert final_state["files"][fail_hash].startswith("failed:"), f"Expected failure status for hash {fail_hash}, got {final_state['files'][fail_hash]}"

    # Check that other pending/failed items were processed (successfully)
    # These hashes correspond to files originally in SSD location, but target is Main
    instr_hash = hash("SSD/ArtistB/Album2/track2_instr.mp3") # Was 'failed', should be 'done' now
    lyrics_hash = hash("SSD/ArtistB/Album2/track2_lyrics.txt") # Was 'pending', should be 'done' now
    assert final_state["files"].get(instr_hash) == "done", f"Expected SSD track instr to be done, got {final_state['files'].get(instr_hash)}"
    assert final_state["files"].get(lyrics_hash) == "done", f"Expected SSD track lyrics to be done, got {final_state['files'].get(lyrics_hash)}"


def test_resume_cli_success(temp_dataset, sample_locations, sample_index_file, sample_state_file):
    """Test the CLI 'resume' command for a successful operation."""
    runner = CliRunner()

    # Mock the underlying resume function to simulate success
    with patch('blackbird.cli.resume_sync_operation', return_value=True) as mock_resume:
        result = runner.invoke(main, [
            'resume',
            str(sample_state_file),
            '--dataset-path', str(temp_dataset) # Explicitly provide dataset path
        ])

    print("CLI Output:", result.output) # Debug output
    print("Exception:", result.exception)

    assert result.exit_code == 0
    assert "Resuming sync operation" in result.output
    assert "Resume operation completed successfully" in result.output
    mock_resume.assert_called_once()
    # Check args passed to the mocked function if needed

def test_resume_cli_failure(temp_dataset, sample_locations, sample_index_file, sample_state_file):
    """Test the CLI 'resume' command for a failed operation."""
    runner = CliRunner()

    # Mock the underlying resume function to simulate failure
    with patch('blackbird.cli.resume_sync_operation', return_value=False) as mock_resume:
        result = runner.invoke(main, [
            'resume',
            str(sample_state_file),
            '--dataset-path', str(temp_dataset)
        ])

    assert result.exit_code == 1 # Should exit with error code
    assert "Resuming sync operation" in result.output
    assert "Resume operation finished with errors" in result.output
    assert "State file kept" in result.output
    mock_resume.assert_called_once()
    # Check args passed to the mocked function if needed


def test_resume_cli_infer_path(temp_dataset, sample_locations, sample_index_file, sample_state_file):
    """Test the CLI 'resume' command inferring the dataset path."""
    runner = CliRunner()

    # We need the state file to be *inside* .blackbird for inference
    bb_dir = temp_dataset / ".blackbird"
    inferred_state_file = bb_dir / sample_state_file.name
    sample_state_file.rename(inferred_state_file) # Move it

    with patch('blackbird.cli.resume_sync_operation', return_value=True) as mock_resume:
         # Run from *within* the dataset directory, without --dataset-path
        # Use isolated_filesystem to simulate running from within temp_dataset
        with runner.isolated_filesystem(temp_dir=temp_dataset) as td:
            # Create the .blackbird structure *within* the isolated filesystem
            bb_dir_iso = Path(td) / ".blackbird"
            bb_dir_iso.mkdir()
            state_file_iso = bb_dir_iso / inferred_state_file.name
            # Copy the state file content into the isolated filesystem
            state_file_iso.write_text(inferred_state_file.read_text())
            
            # The state file path to pass to the CLI is relative to td
            state_file_rel_path = Path(".blackbird") / inferred_state_file.name
            result = runner.invoke(main, [
                'resume',
                str(state_file_rel_path) # Use path relative to isolated FS
            ], catch_exceptions=False) # Easier debugging

    print("CLI Output:", result.output)
    print("Exception:", result.exception)

    assert result.exit_code == 0
    # Path printed might be the temporary path from isolated_filesystem
    # assert f"Inferred dataset path from state file: {temp_dataset}" in result.output # This might fail due to temp paths
    assert "Inferred dataset path from state file:" in result.output
    mock_resume.assert_called_once()
    # Check that the dataset_path passed to mock_resume is correct
    call_args = mock_resume.call_args
    # Check the Path object passed
    assert call_args.kwargs.get('dataset_path') == Path(td)


def test_resume_cli_needs_path(tmp_path, sample_state_file):
    """Test CLI failure when dataset path is needed but not provided/inferrable."""
    runner = CliRunner()

    # Run from a directory *outside* the (non-existent) dataset
    # and the state file is not in a .blackbird subdir relative to CWD
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    moved_state_file = outside_dir / sample_state_file.name
    # We need the original sample_state_file fixture which might be in a different temp dir
    # Let's copy its content instead of moving
    # sample_state_file.rename(moved_state_file)
    moved_state_file.write_text(sample_state_file.read_text())


    # Change CWD for the test
    with runner.isolated_filesystem(temp_dir=outside_dir) as td:
        # Get the Path object for the isolated directory
        td_path = Path(td)
        # Construct the absolute path to the state file within the isolated dir
        state_file_abs_path_in_td = td_path / moved_state_file.name
        
        result = runner.invoke(main, [
            'resume',
            # Pass the absolute path within the isolated filesystem
            str(state_file_abs_path_in_td)
            # No --dataset-path
        ])

    assert result.exit_code != 0 # Should fail because dataset path cannot be inferred
    assert "Warning: Could not infer dataset path" in result.output
    assert "does not appear to be a dataset" in result.output
    assert "Please specify --dataset-path" in result.output

def test_resume_move_not_implemented_cli(temp_dataset, sample_state_file):
    """Test the CLI message for resuming 'move' operations."""
    runner = CliRunner()

    # Modify the state file to be a 'move' operation
    state_data = load_operation_state(sample_state_file)
    assert state_data is not None
    state_data["operation_type"] = "move"
    with open(sample_state_file, "w") as f:
        state_to_save = state_data.copy()
        state_to_save['files'] = {str(k): v for k, v in state_data['files'].items()}
        json.dump(state_to_save, f, indent=2)


    result = runner.invoke(main, [
        'resume',
        str(sample_state_file),
        '--dataset-path', str(temp_dataset)
    ])

    # It currently exits with success code 0 but prints a warning.
    # If it should fail (exit code 1), adjust the test.
    assert result.exit_code == 1, "Should exit with error code 1 for unimplemented operation"
    assert "Resuming move operation" in result.output
    assert "Resuming 'move' operations is not yet implemented" in result.output
    # Depending on whether it exits early or tries to proceed, adjust assertions.
    # assert "Resume operation finished with errors" in result.output # If it exits with code 1
    # Check stderr for the specific error message if needed
    assert "Resume operation finished with errors" in result.output # Check for the final error message

