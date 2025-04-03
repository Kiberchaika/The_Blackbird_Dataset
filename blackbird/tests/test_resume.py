import pytest
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from unittest.mock import patch, MagicMock, call, ANY
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
    get_state_file_path,
    update_operation_state_file # Added update
)
from blackbird.sync import resume_sync_operation, SyncState, WebDAVClient, SyncStats, _process_file_for_resume # Added _process_file_for_resume for more direct testing
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.locations import LocationsManager, resolve_symbolic_path, SymbolicPathError
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

# --- Test Cases for State File Management ---

def test_state_file_creation_loading_deletion(temp_dataset: Path):
    """Verify basic creation, loading, and deletion of state files."""
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

def test_state_file_update(temp_dataset: Path):
    """Test updating the status of a file in the state file."""
    bb_dir = temp_dataset / ".blackbird"
    hashes = [101, 102, 103]
    op_type = "sync"
    source = "webdav://test_update"
    target_loc = "SSD"

    state_file_path = create_operation_state(bb_dir, op_type, source, target_loc, hashes)
    assert state_file_path.exists()

    # Update status of one hash
    update_operation_state_file(state_file_path, 102, "done")

    # Load and verify
    loaded_state = load_operation_state(state_file_path)
    assert loaded_state["files"][101] == "pending"
    assert loaded_state["files"][102] == "done"
    assert loaded_state["files"][103] == "pending"

    # Update another to failed
    fail_reason = "failed: Disk full"
    update_operation_state_file(state_file_path, 103, fail_reason)

    # Load and verify again
    loaded_state = load_operation_state(state_file_path)
    assert loaded_state["files"][101] == "pending"
    assert loaded_state["files"][102] == "done"
    assert loaded_state["files"][103] == fail_reason

    # Test updating non-existent hash (should log warning and not fail)
    with patch('logging.Logger.warning') as mock_warning:
        update_operation_state_file(state_file_path, 999, "done")
        mock_warning.assert_called_once()

    delete_operation_state(state_file_path)


# --- Test Cases for Sync Resume Logic ---

# Helper to create a dummy file
def create_dummy_file(path: Path, size: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(b'x' * size)

# Patch the actual download function used internally by resume logic
@patch('blackbird.sync._process_file_for_resume')
def test_resume_sync_operation_success(
    mock_process_file: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path,
    sample_index_data: Dict[int, Tuple[str, int]]
):
    """Test successful resume where pending/failed items are handled correctly."""
    original_state = load_operation_state(sample_state_file)
    assert original_state is not None
    target_location_name = original_state["target_location"] # Should be "Main"

    # --- Setup pre-existing files ---
    existing_hash = hash("Main/ArtistA/Album1/track_already_exists.mp3")
    existing_sym_path, existing_size = sample_index_data[existing_hash]
    existing_local_path = sample_locations[target_location_name] / Path(existing_sym_path).relative_to("Main")
    create_dummy_file(existing_local_path, existing_size) # Correct size

    mismatch_hash = hash("Main/ArtistA/Album1/track_size_mismatch.mp3")
    mismatch_sym_path, mismatch_expected_size = sample_index_data[mismatch_hash]
    mismatch_local_path = sample_locations[target_location_name] / Path(mismatch_sym_path).relative_to("Main")
    create_dummy_file(mismatch_local_path, mismatch_expected_size + 10) # Incorrect size

    # --- Configure mock for _process_file_for_resume ---
    # It should return: (symbolic_path, size, SyncState, Optional[error_message])
    def process_file_side_effect(client, dataset, symbolic_remote_path, expected_size, target_loc_name, file_hash, profiling_stats):
        # Simulate successful download for pending/failed items
        if file_hash == hash("Main/ArtistA/Album1/track1_vocals.mp3") or \
           file_hash == hash("SSD/ArtistB/Album2/track2_instr.mp3") or \
           file_hash == hash("SSD/ArtistB/Album2/track2_lyrics.txt") or \
           file_hash == hash("Main/ArtistA/Album1/track_size_mismatch.mp3"): # Mismatch will be re-processed
             # Check if the size mismatch file was deleted first
             if file_hash == mismatch_hash:
                  assert not mismatch_local_path.exists()
             create_dummy_file(sample_locations[target_loc_name] / Path(symbolic_remote_path).relative_to(Path(symbolic_remote_path).parts[0]), expected_size)
             return (symbolic_remote_path, expected_size, SyncState.SYNCED, None)
        # This case shouldn't be called for 'done' or correctly existing files
        pytest.fail(f"_process_file_for_resume called unexpectedly for hash {file_hash}")
        # return (symbolic_remote_path, 0, SyncState.FAILED, "Unexpected call") # Should not happen

    mock_process_file.side_effect = process_file_side_effect

    # --- Run the resume function ---
    # Patch delete_operation_state from sync module where it's called
    with patch('blackbird.sync.delete_operation_state') as mock_delete_state, \
         patch('blackbird.sync.configure_client') as mock_configure_client: # Also mock client config
        mock_configure_client.return_value = MagicMock(spec=WebDAVClient) # Provide a mock client

        success = resume_sync_operation(
            dataset_path=temp_dataset,
            state_file_path=sample_state_file,
            state=original_state,
            parallel=1 # Test sequentially first
        )

    # --- Assertions ---
    assert success is True
    mock_delete_state.assert_called_once_with(sample_state_file) # State file deleted on success

    # Verify _process_file_for_resume calls
    # Expected calls:
    # - hash("Main/ArtistA/Album1/track1_vocals.mp3") -> pending
    # - hash("SSD/ArtistB/Album2/track2_instr.mp3") -> failed
    # - hash("SSD/ArtistB/Album2/track2_lyrics.txt") -> pending
    # - hash("Main/ArtistA/Album1/track_size_mismatch.mp3") -> pending (size mismatch triggers re-download)
    # Not called for:
    # - hash("Main/ArtistA/Album1/track1.mir.json") -> done
    # - hash("Main/ArtistA/Album1/track_already_exists.mp3") -> pending but exists correctly
    assert mock_process_file.call_count == 4

    # Check calls more specifically (using ANY for client, dataset, profiling)
    expected_calls = [
        call(ANY, ANY, "Main/ArtistA/Album1/track1_vocals.mp3", 1024, target_location_name, hash("Main/ArtistA/Album1/track1_vocals.mp3"), ANY),
        call(ANY, ANY, "SSD/ArtistB/Album2/track2_instr.mp3", 2048, target_location_name, hash("SSD/ArtistB/Album2/track2_instr.mp3"), ANY),
        call(ANY, ANY, "SSD/ArtistB/Album2/track2_lyrics.txt", 256, target_location_name, hash("SSD/ArtistB/Album2/track2_lyrics.txt"), ANY),
        call(ANY, ANY, "Main/ArtistA/Album1/track_size_mismatch.mp3", 4000, target_location_name, mismatch_hash, ANY),
    ]
    mock_process_file.assert_has_calls(expected_calls, any_order=True) # Order might vary with parallel execution

    # Verify the size mismatch file was correctly overwritten
    assert mismatch_local_path.exists()
    assert mismatch_local_path.stat().st_size == mismatch_expected_size

@patch('blackbird.sync._process_file_for_resume')
def test_resume_sync_operation_failure(
    mock_process_file: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path,
    sample_index_data: Dict[int, Tuple[str, int]]
):
    """Test resume failure where at least one download fails."""
    original_state = load_operation_state(sample_state_file)
    assert original_state is not None
    target_location_name = original_state["target_location"]

    # --- Configure mock to fail one download ---
    fail_hash = hash("SSD/ArtistB/Album2/track2_instr.mp3") # This one was already marked as failed
    fail_message = "Permanent download error"

    def process_file_side_effect(client, dataset, symbolic_remote_path, expected_size, target_loc_name, file_hash, profiling_stats):
        if file_hash == fail_hash:
            return (symbolic_remote_path, 0, SyncState.FAILED, fail_message)
        else:
            # Assume others succeed (size mismatch will be deleted and succeed)
            if file_hash == hash("Main/ArtistA/Album1/track_size_mismatch.mp3"):
                 mismatch_sym_path, mismatch_expected_size = sample_index_data[file_hash]
                 mismatch_local_path = sample_locations[target_location_name] / Path(mismatch_sym_path).relative_to("Main")
                 if mismatch_local_path.exists(): # Simulate deletion before retry
                      mismatch_local_path.unlink()
                 create_dummy_file(mismatch_local_path, mismatch_expected_size)
            elif file_hash not in [hash("Main/ArtistA/Album1/track1.mir.json"), hash("Main/ArtistA/Album1/track_already_exists.mp3")]: # Skip done/existing
                create_dummy_file(sample_locations[target_loc_name] / Path(symbolic_remote_path).relative_to(Path(symbolic_remote_path).parts[0]), expected_size)

            return (symbolic_remote_path, expected_size, SyncState.SYNCED, None)

    mock_process_file.side_effect = process_file_side_effect

    # --- Run the resume function ---
    with patch('blackbird.sync.delete_operation_state') as mock_delete_state, \
         patch('blackbird.sync.configure_client') as mock_configure_client:
        mock_configure_client.return_value = MagicMock(spec=WebDAVClient)
        success = resume_sync_operation(
            dataset_path=temp_dataset,
            state_file_path=sample_state_file,
            state=original_state,
            parallel=1
        )

    # --- Assertions ---
    assert success is False # Should return False if any file failed
    mock_delete_state.assert_not_called() # State file should NOT be deleted on failure
    assert sample_state_file.exists() # Verify file still exists

    # Verify the status in the state file was updated for the failed file
    final_state = load_operation_state(sample_state_file)
    assert final_state is not None
    assert final_state["files"][fail_hash].startswith("failed: Permanent download error") # Check updated status

    # Verify other pending files were marked done
    assert final_state["files"][hash("Main/ArtistA/Album1/track1_vocals.mp3")] == "done"
    assert final_state["files"][hash("SSD/ArtistB/Album2/track2_lyrics.txt")] == "done"
    assert final_state["files"][hash("Main/ArtistA/Album1/track_size_mismatch.mp3")] == "done"
    # Original done/skipped should remain untouched
    assert final_state["files"][hash("Main/ArtistA/Album1/track1.mir.json")] == "done"
    assert final_state["files"][hash("Main/ArtistA/Album1/track_already_exists.mp3")] == "done" # Marked done because it existed

# --- Test Cases for CLI Integration ---

@patch('blackbird.cli.resume_sync_operation')
def test_resume_cli_success(
    mock_resume_sync: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path], # Needed for Dataset init
    sample_index_file: Path,          # Needed for Dataset init
    sample_state_file: Path
):
    """Test the 'blackbird resume' command succeeds."""
    mock_resume_sync.return_value = True # Simulate successful resume logic

    runner = CliRunner()
    result = runner.invoke(main, [
        'resume',
        str(sample_state_file),
        '--dataset-path', str(temp_dataset)
    ])

    assert result.exit_code == 0
    assert "Resuming operation from" in result.output
    assert "Resume completed successfully" in result.output
    mock_resume_sync.assert_called_once()
    # Verify args passed to resume_sync_operation (state is loaded in CLI)
    call_args = mock_resume_sync.call_args[1] # Get kwargs
    assert call_args['dataset_path'] == temp_dataset
    assert call_args['state_file_path'] == sample_state_file
    assert isinstance(call_args['state'], dict) # Check state was loaded and passed


@patch('blackbird.cli.resume_sync_operation')
def test_resume_cli_failure(
    mock_resume_sync: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path
):
    """Test the 'blackbird resume' command fails gracefully."""
    mock_resume_sync.return_value = False # Simulate failed resume logic

    runner = CliRunner()
    result = runner.invoke(main, [
        'resume',
        str(sample_state_file),
        '--dataset-path', str(temp_dataset)
    ])

    assert result.exit_code != 0 # Should exit with non-zero code on failure
    assert "Resuming operation from" in result.output
    assert "Resume failed. State file preserved" in result.output
    mock_resume_sync.assert_called_once()


@patch('blackbird.cli.resume_sync_operation')
def test_resume_cli_infer_dataset_path(
    mock_resume_sync: MagicMock,
    temp_dataset: Path,
    sample_locations: Dict[str, Path],
    sample_index_file: Path,
    sample_state_file: Path
):
    """Test that --dataset-path can be inferred if state file is in .blackbird."""
    mock_resume_sync.return_value = True

    runner = CliRunner()
    # Run from the dataset root directory, providing only the state file path
    with runner.isolated_filesystem(temp_dir=temp_dataset.parent) as td:
        # Recreate necessary structure in isolated env
        iso_dataset_path = Path(td) / temp_dataset.name
        iso_bb_dir = iso_dataset_path / ".blackbird"
        iso_bb_dir.mkdir(parents=True)
        # Copy state file into the isolated .blackbird dir
        iso_state_file = iso_bb_dir / sample_state_file.name
        iso_state_file.write_bytes(sample_state_file.read_bytes())
        # Copy index and locations
        (iso_bb_dir / sample_index_file.name).write_bytes(sample_index_file.read_bytes())
        loc_file_orig = temp_dataset / ".blackbird" / "locations.json"
        (iso_bb_dir / "locations.json").write_bytes(loc_file_orig.read_bytes())


        # Change CWD for the invocation
        os.chdir(iso_dataset_path)

        result = runner.invoke(main, [
            'resume',
            str(iso_state_file) # No --dataset-path
        ], catch_exceptions=False) # Easier debugging

        # Change back CWD
        os.chdir(td) # Go back to root of isolated fs

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "Resuming operation from" in result.output
    assert "Resume completed successfully" in result.output
    mock_resume_sync.assert_called_once()
    call_args = mock_resume_sync.call_args[1]
    # IMPORTANT: Check that the inferred dataset path is correct
    assert call_args['dataset_path'] == iso_dataset_path


def test_resume_cli_needs_path_or_infer(tmp_path):
    """Test CLI errors if dataset path cannot be inferred and isn't provided."""
     # Create a dummy state file outside any .blackbird structure
    dummy_state_path = tmp_path / "operation_sync_123.json"
    dummy_state_path.touch()

    runner = CliRunner()
    # Run from a directory that isn't a dataset dir and state file isn't inside .blackbird
    result = runner.invoke(main, [
        'resume',
        str(dummy_state_path)
    ])

    assert result.exit_code != 0
    assert "Could not automatically determine dataset path" in result.output
    assert "Please provide --dataset-path" in result.output

def test_resume_cli_missing_state_file(temp_dataset: Path):
    """Test CLI errors if the specified state file doesn't exist."""
    runner = CliRunner()
    non_existent_state_file = temp_dataset / ".blackbird" / "non_existent_state.json"
    result = runner.invoke(main, [
        'resume',
        str(non_existent_state_file),
        '--dataset-path', str(temp_dataset)
    ])

    assert result.exit_code != 0
    assert f"State file not found: {non_existent_state_file}" in result.output

@patch('blackbird.cli.load_operation_state')
def test_resume_cli_invalid_state_file(
    mock_load_state: MagicMock,
    temp_dataset: Path,
    sample_state_file: Path # Use it just for the path
):
    """Test CLI errors if the state file is invalid or unreadable."""
    mock_load_state.return_value = None # Simulate load failure

    runner = CliRunner()
    result = runner.invoke(main, [
        'resume',
        str(sample_state_file),
        '--dataset-path', str(temp_dataset)
    ])

    assert result.exit_code != 0
    assert f"Failed to load or parse state file: {sample_state_file}" in result.output
    mock_load_state.assert_called_once_with(sample_state_file)

# Skip move tests for now as move is not implemented
# @patch('blackbird.cli.resume_move_operation') # Assume this exists later
# def test_resume_cli_move_not_implemented(mock_resume_move, temp_dataset):
#     """Test that attempting to resume a 'move' operation fails correctly (for now)."""
#     bb_dir = temp_dataset / ".blackbird"
#     # Create a dummy state file for a move operation
#     move_state_data: OperationState = {
#         "operation_type": "move",
#         "timestamp": time.time(),
#         "source": "Main",
#         "target_location": "SSD",
#         "components": None,
#         "files": {str(hash("a/b/c")): "pending"}
#     }
#     move_state_file = get_state_file_path(bb_dir, "move", move_state_data['timestamp'])
#     with open(move_state_file, "w") as f:
#         json.dump(move_state_data, f)

#     runner = CliRunner()
#     result = runner.invoke(main, [
#         'resume',
#         str(move_state_file),
#         '--dataset-path', str(temp_dataset)
#     ])

#     assert result.exit_code != 0
#     assert "Resuming 'move' operations is not yet supported" in result.output
#     mock_resume_move.assert_not_called()

