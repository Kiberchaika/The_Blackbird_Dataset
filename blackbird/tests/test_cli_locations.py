import pytest
import json
from pathlib import Path
from click.testing import CliRunner
from blackbird.cli import main # Assuming your main CLI entry point is here
from blackbird.locations import LocationsManager

# Use specific paths mentioned in the plan for testing existence
EXISTING_TEST_DIR_1 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_2")
EXISTING_TEST_DIR_2 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_3")
NON_EXISTENT_DIR = Path("/tmp/non_existent_blackbird_test_cli_dir")

# Ensure test directories exist for path validation tests
@pytest.fixture(scope="session", autouse=True)
def ensure_test_dirs():
    EXISTING_TEST_DIR_1.mkdir(exist_ok=True)
    EXISTING_TEST_DIR_2.mkdir(exist_ok=True)
    if NON_EXISTENT_DIR.exists():
        NON_EXISTENT_DIR.rmdir() # Ensure it doesn't exist before tests

@pytest.fixture
def temp_dataset_dir(tmp_path):
    """Creates a temporary dataset directory structure."""
    dataset_root = tmp_path / "dataset_cli"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)
    # Create a dummy schema, as Dataset init might need it
    schema_path = blackbird_dir / "schema.json"
    schema_path.write_text(json.dumps({"version": "1.0", "components": {}}))
    return dataset_root

@pytest.fixture
def locations_json_path(temp_dataset_dir):
    """Path to the locations.json file within the temp dataset dir."""
    return temp_dataset_dir / ".blackbird" / "locations.json"

@pytest.fixture
def runner():
    """Provides a CliRunner instance."""
    return CliRunner()

def test_location_list_default(runner, temp_dataset_dir):
    """Test `location list` with default Main location."""
    result = runner.invoke(main, ['location', 'list', str(temp_dataset_dir)])
    assert result.exit_code == 0
    # Check for the core part of the output, matching the default padding
    assert f"Main : {temp_dataset_dir.resolve()}" in result.output

def test_location_list_multiple(runner, temp_dataset_dir, locations_json_path):
    """Test `location list` with multiple locations defined in json."""
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Backup": str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))
    
    result = runner.invoke(main, ['location', 'list', str(temp_dataset_dir)])
    assert result.exit_code == 0
    # Check for both locations, using correct padding based on max name length
    # 'Main' (len 4) padded to max_len (6) gets 2 spaces
    assert f"Main   : {temp_dataset_dir.resolve()}" in result.output 
    # 'Backup' (len 6) padded to max_len (6) gets 0 spaces
    assert f"Backup : {EXISTING_TEST_DIR_1.resolve()}" in result.output

def test_location_add_success(runner, temp_dataset_dir, locations_json_path):
    """Test `location add` successfully adds a new location."""
    name = "SSD_Fast"
    path_str = str(EXISTING_TEST_DIR_1)
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, path_str])
    
    # Should succeed with exit code 0
    assert result.exit_code == 0 
    assert f"Location '{name}' added successfully." in result.output
    
    # Verify save
    assert locations_json_path.exists()
    saved_data = json.loads(locations_json_path.read_text())
    assert name in saved_data
    assert saved_data[name] == str(EXISTING_TEST_DIR_1.resolve())

def test_location_add_invalid_path(runner, temp_dataset_dir, locations_json_path):
    """Test `location add` with a non-existent path."""
    name = "InvalidPath"
    path_str = str(NON_EXISTENT_DIR) 
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, path_str])

    assert result.exit_code != 0
    # Check for the specific error from LocationsManager passed through CLI
    assert f"Error adding location: Path '{path_str}' does not exist" in result.output

def test_location_add_duplicate_name(runner, temp_dataset_dir):
    """Test `location add` with a duplicate name ('Main')."""
    path_str = str(EXISTING_TEST_DIR_1)
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), "Main", path_str])

    assert result.exit_code != 0
    # Check for the specific error from LocationsManager passed through CLI
    assert f"Error adding location: Location name 'Main' already exists" in result.output

def test_location_remove_success(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` successfully removes a location."""
    name_to_remove = "ToDelete"
    initial_loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(initial_loc_data))

    # Confirm removal with 'y'
    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='y\n')

    assert result.exit_code == 0
    assert f"Location '{name_to_remove}' removed successfully." in result.output

    # Verify save
    saved_data = json.loads(locations_json_path.read_text())
    assert name_to_remove not in saved_data
    assert "Main" in saved_data # Main should still be there

def test_location_remove_abort(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` aborts when user inputs 'n'."""
    name_to_remove = "KeepMe"
    initial_loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(initial_loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='n\n')

    # Click's confirmation_option abort raises click.Abort, which translates to exit code 1
    assert result.exit_code == 1 
    assert "Aborted!" in result.output # Default abort message from Click

    # Verify location was NOT removed
    saved_data = json.loads(locations_json_path.read_text())
    assert name_to_remove in saved_data

def test_location_remove_non_existent(runner, temp_dataset_dir):
    """Test `location remove` for a location that doesn't exist."""
    # Input 'y' to bypass confirmation, the error should happen before that
    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "NotFound"], input='y\n') 

    assert result.exit_code != 0
    # Check for the specific error from LocationsManager passed through CLI
    assert "Error removing location: Location 'NotFound' does not exist" in result.output

def test_location_remove_main_disallowed(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` attempting to remove 'Main' (should be allowed if others exist)."""
    # Add another location so 'Main' isn't the only one
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Other": str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "Main"], input='y\n')

    # Should succeed
    assert result.exit_code == 0
    assert "Location 'Main' removed successfully." in result.output

    # Verify 'Main' was actually removed
    saved_data = json.loads(locations_json_path.read_text())
    assert "Main" not in saved_data
    assert "Other" in saved_data

def test_location_remove_main_when_only_one(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` fails when trying to remove 'Main' as the only location."""
    # Ensure only Main exists (default state)
    if locations_json_path.exists(): locations_json_path.unlink()

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "Main"], input='y\n')

    assert result.exit_code != 0
    assert "Error removing location: Cannot remove the default location 'Main' when it is the only location." in result.output 