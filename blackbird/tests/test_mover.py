import pytest
import shutil
import os
from pathlib import Path
import pickle
import time
from unittest.mock import patch, MagicMock

from blackbird.index import DatasetIndex, TrackInfo
from blackbird.dataset import Dataset
from blackbird.locations import LocationsManager, resolve_symbolic_path, SymbolicPathError
from blackbird.mover import move_data
from blackbird.operations import create_operation_state, load_operation_state, update_operation_state_file, delete_operation_state

# Constants for location names
LOC_MAIN = "Main"
LOC_SSD = "SSD_Fast"
LOC_HDD = "HDD_Slow"

# Define paths relative to a base temp path provided by pytest fixture
@pytest.fixture(scope="function")
def setup_test_environment(tmp_path):
    """Sets up a multi-location dataset environment in a temporary directory."""
    base_path = tmp_path / "multi_loc_dataset"
    main_loc_path = base_path # Main location is the dataset root itself
    ssd_loc_path = tmp_path / "ssd_storage"
    hdd_loc_path = tmp_path / "hdd_storage"

    # Create directories
    bb_dir = base_path / ".blackbird"
    for p in [main_loc_path, ssd_loc_path, hdd_loc_path, bb_dir]:
        p.mkdir(parents=True, exist_ok=True)

    # Create locations.json
    locations_data = {
        LOC_MAIN: str(main_loc_path),
        LOC_SSD: str(ssd_loc_path),
        LOC_HDD: str(hdd_loc_path),
    }
    locations_file = bb_dir / "locations.json"
    with open(locations_file, 'w') as f:
        import json
        json.dump(locations_data, f)

    # Create sample data files in different locations
    (main_loc_path / "Artist1" / "AlbumA").mkdir(parents=True, exist_ok=True)
    (main_loc_path / "Artist1" / "AlbumA" / "track1_vocals.flac").write_text("main_data1")
    (main_loc_path / "Artist1" / "AlbumA" / "track1_drums.flac").write_text("main_data2")

    (ssd_loc_path / "Artist2" / "AlbumB").mkdir(parents=True, exist_ok=True)
    (ssd_loc_path / "Artist2" / "AlbumB" / "track2_full.mp3").write_text("ssd_data1_long" * 10) # Larger file

    (hdd_loc_path / "Artist1" / "AlbumC").mkdir(parents=True, exist_ok=True)
    (hdd_loc_path / "Artist1" / "AlbumC" / "track3_other.wav").write_text("hdd_data")

    # Create a dummy DatasetIndex reflecting this structure
    index = DatasetIndex(
        last_updated=time.time(),
        stats_by_location={
            LOC_MAIN: {'files': 2, 'size': 20, 'tracks': 1, 'albums': 1, 'artists': 1},
            LOC_SSD: {'files': 1, 'size': 140, 'tracks': 1, 'albums': 1, 'artists': 1},
            LOC_HDD: {'files': 1, 'size': 8, 'tracks': 1, 'albums': 1, 'artists': 1},
        },
        file_info_by_hash={},
        tracks={},
        track_by_album={},
        album_by_artist={},
        total_size=168 # Manually calculate or sum from stats
    )

    # Manually populate index data (hashes, files, etc.)
    files_info = [
        (f"{LOC_MAIN}/Artist1/AlbumA/track1_vocals.flac", 10),
        (f"{LOC_MAIN}/Artist1/AlbumA/track1_drums.flac", 10),
        (f"{LOC_SSD}/Artist2/AlbumB/track2_full.mp3", 140),
        (f"{LOC_HDD}/Artist1/AlbumC/track3_other.wav", 8),
    ]

    track1_path = f"{LOC_MAIN}/Artist1/AlbumA/track1"
    track2_path = f"{LOC_SSD}/Artist2/AlbumB/track2"
    track3_path = f"{LOC_HDD}/Artist1/AlbumC/track3"

    index.tracks = {
        track1_path: TrackInfo(track_path=track1_path, artist="Artist1", album_path=f"{LOC_MAIN}/Artist1/AlbumA", base_name="track1", cd_number=None, files={}, file_sizes={}),
        track2_path: TrackInfo(track_path=track2_path, artist="Artist2", album_path=f"{LOC_SSD}/Artist2/AlbumB", base_name="track2", cd_number=None, files={}, file_sizes={}),
        track3_path: TrackInfo(track_path=track3_path, artist="Artist1", album_path=f"{LOC_HDD}/Artist1/AlbumC", base_name="track3", cd_number=None, files={}, file_sizes={}),
    }
    index.track_by_album = {
        f"{LOC_MAIN}/Artist1/AlbumA": [track1_path],
        f"{LOC_SSD}/Artist2/AlbumB": [track2_path],
        f"{LOC_HDD}/Artist1/AlbumC": [track3_path],
    }
    index.album_by_artist = {
        "Artist1": [f"{LOC_MAIN}/Artist1/AlbumA", f"{LOC_HDD}/Artist1/AlbumC"],
        "Artist2": [f"{LOC_SSD}/Artist2/AlbumB"],
    }

    for symbolic_path, size in files_info:
        file_hash = hash(symbolic_path)
        index.file_info_by_hash[file_hash] = (symbolic_path, size)
        # Simplified component mapping for testing mover
        if "track1" in symbolic_path:
            track_path = track1_path
            component = "vocals" if "vocals" in symbolic_path else "drums"
        elif "track2" in symbolic_path:
            track_path = track2_path
            component = "full"
        else:
            track_path = track3_path
            component = "other"
        index.tracks[track_path].files[component] = symbolic_path
        index.tracks[track_path].file_sizes[component] = size

    # Save the index
    index_file = bb_dir / "index.pickle"
    with open(index_file, 'wb') as f:
        pickle.dump(index, f)

    # Create Dataset object
    dataset = Dataset(base_path)

    return dataset, main_loc_path, ssd_loc_path, hdd_loc_path

# --- Test move_data Function --- #

@patch('shutil.move')
@patch('blackbird.operations.delete_operation_state') # Prevent auto-deletion by mover
@patch('blackbird.mover.Dataset.rebuild_index') # Mock rebuild at mover level
def test_move_by_size_limit(mock_rebuild, mock_delete_state, mock_shutil_move, setup_test_environment):
    """Test moving files based on a size limit."""
    dataset, main_path, ssd_path, hdd_path = setup_test_environment
    source_loc = LOC_SSD
    target_loc = LOC_MAIN
    size_limit_gb = 0.00000015 # ~150 bytes, should select the SSD file

    # Expected source and target absolute paths for the SSD file
    expected_source_abs = ssd_path / "Artist2" / "AlbumB" / "track2_full.mp3"
    expected_target_abs = main_path / "Artist2" / "AlbumB" / "track2_full.mp3"

    move_stats = move_data(
        dataset=dataset,
        source_location_name=source_loc,
        target_location_name=target_loc,
        size_limit_gb=size_limit_gb,
        dry_run=False
    )

    assert move_stats["moved_files"] == 1
    state_file_path = move_stats.get("state_file_path")
    mock_shutil_move.assert_called_once_with(str(expected_source_abs), str(expected_target_abs))
    # Check state file content before deletion
    assert state_file_path is not None
    assert state_file_path.exists() # Ensure it exists before loading
    state_data = load_operation_state(state_file_path)
    assert state_data["operation_type"] == "move"
    assert state_data["source"] == source_loc
    assert state_data["target_location"] == target_loc
    assert state_data["files"]
    file_hash = hash(f"{LOC_SSD}/Artist2/AlbumB/track2_full.mp3")
    assert file_hash in state_data["files"]
    assert state_data["files"][file_hash] == "done"

    # Check state file is NOT deleted by move_data due to patch,
    # but verify that move_data attempted to delete it on success.
    mock_delete_state.assert_called_once_with(state_file_path)
    assert state_file_path.exists() # Should still exist because delete is mocked
    # Manually clean up
    state_file_path.unlink()
    assert not state_file_path.exists()

    # Rebuild index is NOT called by move_data itself, but by the CLI wrapper
    mock_rebuild.assert_not_called()

@patch('shutil.move')
@patch('blackbird.operations.delete_operation_state') # Prevent auto-deletion by mover
@patch('blackbird.mover.Dataset.rebuild_index')
def test_move_specific_folders(mock_rebuild, mock_delete_state, mock_shutil_move, setup_test_environment):
    """Test moving specific folders."""
    dataset, main_path, ssd_path, hdd_path = setup_test_environment
    source_loc = LOC_MAIN
    target_loc = LOC_HDD
    folders_to_move = ["Artist1/AlbumA"]

    # Expected moves
    expected_moves = [
        (main_path / "Artist1" / "AlbumA" / "track1_vocals.flac",
         hdd_path / "Artist1" / "AlbumA" / "track1_vocals.flac"),
        (main_path / "Artist1" / "AlbumA" / "track1_drums.flac",
         hdd_path / "Artist1" / "AlbumA" / "track1_drums.flac"),
    ]

    move_stats = move_data(
        dataset=dataset,
        source_location_name=source_loc,
        target_location_name=target_loc,
        specific_folders=folders_to_move,
        dry_run=False
    )

    assert move_stats["moved_files"] == 2
    state_file_path = move_stats.get("state_file_path")
    assert mock_shutil_move.call_count == 2
    # Check calls carefully, order might vary depending on iteration
    call_args_list = [call[0] for call in mock_shutil_move.call_args_list]
    assert (str(expected_moves[0][0]), str(expected_moves[0][1])) in call_args_list
    assert (str(expected_moves[1][0]), str(expected_moves[1][1])) in call_args_list

    assert state_file_path is not None
    assert state_file_path.exists() # Check it exists

    # Check state file is NOT deleted by move_data due to patch,
    # but verify that move_data attempted to delete it on success.
    mock_delete_state.assert_called_once_with(state_file_path)
    assert state_file_path.exists() # Should still exist because delete is mocked
    # Manually clean up
    state_file_path.unlink()
    assert not state_file_path.exists()

    mock_rebuild.assert_not_called()

@patch('shutil.move')
@patch('blackbird.mover.Dataset.rebuild_index')
def test_move_dry_run(mock_rebuild, mock_shutil_move, setup_test_environment):
    """Test dry run does not move files or create state file."""
    dataset, *_ = setup_test_environment

    move_stats = move_data(
        dataset=dataset,
        source_location_name=LOC_MAIN,
        target_location_name=LOC_SSD,
        specific_folders=["Artist1/AlbumA"],
        dry_run=True
    )

    assert move_stats["skipped_files"] == 2 # Dry run counts as skipped
    assert move_stats.get("state_file_path") is None # No state file created
    mock_shutil_move.assert_not_called() # But not moved
    mock_rebuild.assert_not_called()

@patch('shutil.move', side_effect=OSError("Disk full"))
@patch('blackbird.operations.delete_operation_state') # Keep mock to prevent deletion in case of partial success before error
@patch('blackbird.mover.Dataset.rebuild_index')
def test_move_interruption_handling(mock_rebuild, mock_delete_state, mock_shutil_move, setup_test_environment):
    """Test that state file is kept when an error occurs during move."""
    dataset, main_path, ssd_path, _ = setup_test_environment
    source_loc = LOC_MAIN
    target_loc = LOC_SSD
    folders_to_move = ["Artist1/AlbumA"] # 2 files

    # The function now catches the OSError internally and returns stats
    move_stats = move_data(
        dataset=dataset,
        source_location_name=source_loc,
        target_location_name=target_loc,
        specific_folders=folders_to_move,
        dry_run=False
    )

    mock_shutil_move.assert_called_once() # Only the first attempt was made
    mock_delete_state.assert_not_called() # State file should NOT be deleted on error

    # Check the returned stats
    assert move_stats["moved_files"] == 0
    assert move_stats["failed_files"] >= 1 # At least one failure occurred
    state_file_path = move_stats.get("state_file_path")
    assert state_file_path is not None, "State file path should be returned in stats on error"
    assert state_file_path.exists() # State file must exist

    state_data = load_operation_state(state_file_path)
    assert state_data["source"] == source_loc
    assert state_data["target_location"] == target_loc
    failed_hash = hash(f"{LOC_MAIN}/Artist1/AlbumA/track1_vocals.flac") # Assuming order
    pending_hash = hash(f"{LOC_MAIN}/Artist1/AlbumA/track1_drums.flac") # Assuming order
    # We need to be careful about assuming the order mock_shutil_move was called in
    # Check which file caused the failure based on the call args
    failed_file_path = mock_shutil_move.call_args[0][0]
    # Use get_all_locations() here
    all_locs = dataset.locations.get_all_locations()
    failed_symbolic = next(sp for h, (sp, sz) in dataset.index.file_info_by_hash.items() if resolve_symbolic_path(sp, all_locs) == Path(failed_file_path))
    failed_hash_actual = hash(failed_symbolic)
    # And here for consistency, though not strictly necessary if only source_loc is used
    pending_symbolic = next(sp for h, (sp, sz) in dataset.index.file_info_by_hash.items() if h != failed_hash_actual and sp.startswith(f"{source_loc}/Artist1/AlbumA"))
    pending_hash_actual = hash(pending_symbolic)


    assert state_data["files"][failed_hash_actual].startswith("failed: Disk full")
    assert state_data["files"][pending_hash_actual] == "pending"

    # Clean up the created state file
    state_file_path.unlink()
    mock_rebuild.assert_not_called()


def test_move_invalid_location(setup_test_environment):
    """Test moving to/from an invalid location name."""
    dataset, *_ = setup_test_environment
    # Update regex to match the actual error message format
    with pytest.raises(ValueError, match=r"Source location '.*' not found in dataset configuration."):
        move_data(dataset=dataset, source_location_name="BadLoc", target_location_name=LOC_MAIN)
    with pytest.raises(ValueError, match=r"Target location '.*' not found in dataset configuration."):
        move_data(dataset=dataset, source_location_name=LOC_MAIN, target_location_name="BadLoc")
    with pytest.raises(ValueError, match="Source and target locations cannot be the same"):
        move_data(dataset=dataset, source_location_name=LOC_MAIN, target_location_name=LOC_MAIN)


def test_move_no_files_found(setup_test_environment):
    """Test moving when no files match the criteria."""
    dataset, *_ = setup_test_environment
    move_stats_1 = move_data(
        dataset=dataset,
        source_location_name=LOC_MAIN,
        target_location_name=LOC_SSD,
        specific_folders=["NonExistentArtist"]
    )
    assert move_stats_1["moved_files"] == 0
    assert move_stats_1.get("state_file_path") is None # No operation started

    move_stats_2 = move_data(
        dataset=dataset,
        source_location_name=LOC_SSD, # Only has 1 file > 100 bytes
        target_location_name=LOC_MAIN,
        size_limit_gb=0.00000001 # ~10 bytes
    )
    assert move_stats_2["moved_files"] == 0
    assert move_stats_2.get("state_file_path") is None

# --- Resume logic is implicitly tested via interruption test + resume tests --- #
# We don't add explicit resume tests here as mover.py doesn't have a resume function
# The resume logic lives in the CLI/sync/resume modules. 