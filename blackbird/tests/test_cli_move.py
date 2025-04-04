import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
import os
from pathlib import Path
import json
import time

from blackbird.cli import main as blackbird_cli # Renamed import

# Reuse fixtures from test_mover if they are broadly applicable
# For simplicity here, we might redefine minimal ones or mock more heavily
TEST_LOC1_NAME = "TestLoc1CLI"
TEST_LOC2_NAME = "TestLoc2CLI"

@pytest.fixture(scope="function") # Use function scope for CLI tests to isolate runs
def temp_cli_dataset(tmp_path):
    """Creates a temporary dataset structure for CLI tests."""
    base_path = tmp_path / "cli_dataset"
    loc1_path = tmp_path / "cli_loc1"
    loc2_path = tmp_path / "cli_loc2"

    for p in [base_path, loc1_path, loc2_path]:
        p.mkdir(parents=True, exist_ok=True)

    # Create dummy index and locations file
    bb_dir = base_path / ".blackbird"
    bb_dir.mkdir()

    # locations.json
    locations_data = {
        TEST_LOC1_NAME: str(loc1_path),
        TEST_LOC2_NAME: str(loc2_path),
    }
    locations_file = bb_dir / "locations.json"
    with open(locations_file, 'w') as f:
        json.dump(locations_data, f)

    # dummy index.pickle (content doesn't matter much as move_data is mocked)
    index_file = bb_dir / "index.pickle"
    index_data = { # Minimal structure
        "dataset_path": str(base_path),
        "schema": {"components": {}},
        "last_updated": time.time(),
        "stats_by_location": {TEST_LOC1_NAME: {}, TEST_LOC2_NAME: {}},
        "file_info_by_hash": {},
        "tracks": {},
        "track_by_album": {},
        "album_by_artist": {},
    }
    import pickle
    with open(index_file, 'wb') as f:
        pickle.dump(index_data, f)

    # Create dummy files (optional, helps if not mocking everything)
    (loc1_path / "ArtistA" / "AlbumX").mkdir(parents=True, exist_ok=True)
    (loc1_path / "ArtistA" / "AlbumX" / "file1.wav").touch()

    return base_path

@pytest.fixture
def runner():
    return CliRunner()

# --- Test Functions ---

@patch('blackbird.cli.move_data')
@patch('blackbird.cli.Dataset.rebuild_index') # Mock rebuild_index in cli context
def test_cli_location_balance(mock_rebuild, mock_move_data, runner, temp_cli_dataset):
    """Test the 'location balance' command."""
    # Simulate move_data returning some dummy hashes and a state file path
    mock_state_file = Path(temp_cli_dataset) / ".blackbird" / "op_state.json"
    mock_state_file.touch() # Create dummy state file for deletion check
    mock_stats = {
        "moved_files": 2,
        "skipped_files": 0,
        "failed_files": 0,
        "total_bytes_moved": 150,
        "identified_files": 2,
        "files_to_move": {123:("...",100), 456:("...",50)}, # Example hashes
        "state_file_path": mock_state_file
    }
    mock_move_data.return_value = mock_stats

    result = runner.invoke(blackbird_cli, [
        'location', 'balance', str(temp_cli_dataset),
        TEST_LOC1_NAME, TEST_LOC2_NAME, '--size', '0.1' # 0.1 GB
    ], input='y\n') # Provide input to confirm manual prompt

    print(f"CLI Output:\n{result.output}")
    print(f"CLI Exception:\n{result.exception}")
    assert result.exit_code == 0
    mock_move_data.assert_called_once()
    # Check args passed to move_data
    call_args, call_kwargs = mock_move_data.call_args
    assert call_kwargs.get('dataset') is not None and call_kwargs['dataset'].path == temp_cli_dataset
    assert call_kwargs.get('source_location_name') == TEST_LOC1_NAME
    assert call_kwargs.get('target_location_name') == TEST_LOC2_NAME
    assert call_kwargs.get('size_limit_gb') == 0.1
    assert call_kwargs.get('specific_folders') is None
    assert call_kwargs.get('dry_run') is False

    assert f"Attempting to move approximately" in result.output # Check for actual log message
    assert "Move operation complete!" in result.output
    assert "Rebuilding index..." in result.output # Corrected assertion
    mock_rebuild.assert_called_once()
    # Assert state file was deleted by the command after successful move
    # Note: state file deletion now happens in mover.py, not CLI
    # The mock_state_file path isn't the one mover uses, so we can't check deletion here directly.
    # We rely on test_mover.py to verify state file deletion logic.

@patch('blackbird.cli.move_data')
@patch('blackbird.cli.Dataset.rebuild_index')
def test_cli_location_balance_dry_run(mock_rebuild, mock_move_data, runner, temp_cli_dataset):
    """Test the 'location balance' command with --dry-run."""
    # Dry run shouldn't create state file, return None
    mock_stats_dry = {
        "moved_files": 0, # Dry run doesn't move
        "skipped_files": 2, # Dry run skips
        "failed_files": 0,
        "total_bytes_moved": 0,
        "identified_files": 2,
        "files_to_move": {123:("...",100), 456:("...",50)},
        "total_bytes_to_move": 150,
        "state_file_path": None
    }
    mock_move_data.return_value = mock_stats_dry

    result = runner.invoke(blackbird_cli, [
        'location', 'balance', str(temp_cli_dataset),
        TEST_LOC1_NAME, TEST_LOC2_NAME, '--size', '0.1', '--dry-run'
    ]) # Dry run shouldn't need input

    assert result.exit_code == 0
    mock_move_data.assert_called_once()
    call_args, call_kwargs = mock_move_data.call_args
    assert call_kwargs.get('dry_run') is True
    assert call_kwargs.get('source_location_name') == TEST_LOC1_NAME
    assert call_kwargs.get('target_location_name') == TEST_LOC2_NAME
    assert "Dry run complete." in result.output # Match CLI dry run output
    assert "Would have skipped 2 files." in result.output # Match CLI dry run output
    assert "Move operation complete." not in result.output # Not actually moved
    assert "Rebuilding index..." not in result.output
    mock_rebuild.assert_not_called()

@patch('blackbird.cli.move_data')
@patch('blackbird.cli.Dataset.rebuild_index')
def test_cli_location_move_folders(mock_rebuild, mock_move_data, runner, temp_cli_dataset):
    """Test the 'location move-folders' command."""
    mock_state_file = Path(temp_cli_dataset) / ".blackbird" / "op_state.json"
    mock_state_file.touch()
    mock_stats_folders = {
        "moved_files": 1,
        "skipped_files": 0,
        "failed_files": 0,
        "total_bytes_moved": 50,
        "identified_files": 1,
        "files_to_move": {789:("...",50)},
        "state_file_path": mock_state_file
    }
    mock_move_data.return_value = mock_stats_folders
    folders_to_move_list = ["ArtistA/AlbumX", "ArtistB"]
    folders_to_move_str = ",".join(folders_to_move_list)

    # Construct command list: options first, then positionals
    cmd_list = [
        'location', 'move-folders',
        '--source-location', TEST_LOC1_NAME,
        str(temp_cli_dataset),  # dataset_path
        TEST_LOC2_NAME,        # target_loc
        folders_to_move_str    # folders_str (single argument)
    ]

    result = runner.invoke(blackbird_cli, cmd_list, input='y\n') # Provide input for manual prompt

    print(f"CLI Output:\n{result.output}")
    print(f"CLI Exception:\n{result.exception}")
    assert result.exit_code == 0
    mock_move_data.assert_called_once()
    call_args, call_kwargs = mock_move_data.call_args
    assert call_kwargs.get('dataset') is not None and call_kwargs['dataset'].path == temp_cli_dataset
    assert call_kwargs.get('source_location_name') == TEST_LOC1_NAME
    assert call_kwargs.get('target_location_name') == TEST_LOC2_NAME
    assert call_kwargs.get('size_limit_gb') is None
    # Check that the split list was passed to move_data
    assert call_kwargs.get('specific_folders') == folders_to_move_list
    assert call_kwargs.get('dry_run') is False

    assert f"Attempting to move folders" in result.output
    assert "Moved 1 files" in result.output
    assert "Rebuilding index..." in result.output
    mock_rebuild.assert_called_once()
    # State file deletion checked in test_mover.py

@patch('blackbird.cli.move_data')
@patch('blackbird.cli.Dataset.rebuild_index')
def test_cli_location_move_folders_dry_run(mock_rebuild, mock_move_data, runner, temp_cli_dataset):
    """Test the 'location move-folders' command with --dry-run."""
    mock_stats_folders_dry = {
        "moved_files": 0,
        "skipped_files": 1,
        "failed_files": 0,
        "total_bytes_moved": 0,
        "identified_files": 1,
        "files_to_move": {789:("...",50)},
        "total_bytes_to_move": 50,
        "state_file_path": None
    }
    mock_move_data.return_value = mock_stats_folders_dry
    folders_to_move_list = ["ArtistA/AlbumX"]
    folders_to_move_str = ",".join(folders_to_move_list)

    # Construct command list: options first, then positionals
    cmd_list = [
        'location', 'move-folders',
        '--source-location', TEST_LOC1_NAME,
        '--dry-run',
        str(temp_cli_dataset),  # dataset_path
        TEST_LOC2_NAME,        # target_loc
        folders_to_move_str    # folders_str (single argument)
    ]

    result = runner.invoke(blackbird_cli, cmd_list) # No input needed for dry run

    assert result.exit_code == 0
    mock_move_data.assert_called_once()
    call_args, call_kwargs = mock_move_data.call_args
    assert call_kwargs.get('dry_run') is True
    assert call_kwargs.get('source_location_name') == TEST_LOC1_NAME
    assert call_kwargs.get('target_location_name') == TEST_LOC2_NAME
    # Check that the split list was passed to move_data
    assert call_kwargs.get('specific_folders') == folders_to_move_list
    assert "Dry run complete." in result.output # Adjusted assertion
    assert "Would have skipped 1 files." in result.output # Adjusted assertion
    assert "Move operation complete." not in result.output
    assert "Rebuilding index..." not in result.output
    mock_rebuild.assert_not_called()

@patch('blackbird.cli.move_data', side_effect=ValueError("Invalid source"))
# @patch('click.confirm') # No longer needed as confirm is conditional
def test_cli_location_move_error_handling(mock_move_data, runner, temp_cli_dataset):
    """Test error handling in CLI move commands."""
    # mock_confirm.return_value = True # No longer needed

    result = runner.invoke(blackbird_cli, [
        'location', 'balance', str(temp_cli_dataset),
        'BadSource', TEST_LOC2_NAME, '--size', '0.1'
    ], input='y\n') # Input 'y' to get past manual confirm and hit the error

    assert result.exit_code != 0 # Expect non-zero exit code on error
    assert "Error:" in result.output
    assert "Invalid source" in result.output

@patch('blackbird.cli.move_data')
@patch('blackbird.cli.Dataset.rebuild_index')
def test_cli_location_move_aborted(mock_rebuild, mock_move_data, runner, temp_cli_dataset):
    """Test aborting the move via confirmation."""
    result = runner.invoke(blackbird_cli, [
        'location', 'balance', str(temp_cli_dataset),
        TEST_LOC1_NAME, TEST_LOC2_NAME, '--size', '0.1'
    ], input='n\n') # Provide 'n' to abort manual prompt

    assert result.exit_code != 0 # Aborted operations usually have non-zero code
    assert "Operation aborted." in result.output # Match new abort message
    mock_move_data.assert_not_called()
    mock_rebuild.assert_not_called() 