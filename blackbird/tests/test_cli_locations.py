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
    assert f"Main: {str(temp_dataset_dir)}" in result.output

def test_location_list_multiple(runner, temp_dataset_dir, locations_json_path):
    """Test `location list` with multiple locations defined in json."""
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Backup": str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    result = runner.invoke(main, ['location', 'list', str(temp_dataset_dir)])
    assert result.exit_code == 0
    assert f"Main: {str(temp_dataset_dir)}" in result.output
    assert f"Backup: {str(EXISTING_TEST_DIR_1)}" in result.output

def test_location_add_success(runner, temp_dataset_dir, locations_json_path):
    """Test `location add` successfully adds a new location."""
    name = "SSD_Fast"
    path_str = str(EXISTING_TEST_DIR_1)
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, path_str])

    assert result.exit_code == 0
    assert f"Location '{name}' added with path {path_str}" in result.output
    assert locations_json_path.exists()
    loaded_data = json.loads(locations_json_path.read_text())
    assert loaded_data == {
        "Main": str(temp_dataset_dir),
        name: path_str
    }

def test_location_add_invalid_path(runner, temp_dataset_dir, locations_json_path):
    """Test `location add` with a non-existent path."""
    name = "InvalidPath"
    path_str = str(NON_EXISTENT_DIR)
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, path_str])

    assert result.exit_code != 0
    assert f"Error: Path does not exist or is not a directory: {path_str}" in result.output
    # Check that the locations file wasn't created or modified incorrectly
    if locations_json_path.exists():
        loaded_data = json.loads(locations_json_path.read_text())
        assert name not in loaded_data
    else:
        # If it didn't exist initially, adding should have failed before saving
        pass

def test_location_add_duplicate_name(runner, temp_dataset_dir):
    """Test `location add` with a duplicate name ('Main')."""
    path_str = str(EXISTING_TEST_DIR_1)
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), "Main", path_str])

    assert result.exit_code != 0
    assert "Error: Location 'Main' already exists" in result.output

def test_location_remove_success(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` successfully removes a location."""
    name_to_remove = "ToDelete"
    loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    # Use input 'y' to confirm deletion
    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='y\n')

    assert result.exit_code == 0
    assert f"Location '{name_to_remove}' removed." in result.output
    loaded_data = json.loads(locations_json_path.read_text())
    assert name_to_remove not in loaded_data
    assert loaded_data == {"Main": str(temp_dataset_dir)}

def test_location_remove_abort(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` aborts when user inputs 'n'."""
    name_to_remove = "KeepMe"
    initial_loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(initial_loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='n\n')

    assert result.exit_code == 0 # Aborting is a successful exit
    assert "Operation aborted." in result.output
    # Verify the file wasn't changed
    loaded_data = json.loads(locations_json_path.read_text())
    assert loaded_data == initial_loc_data

def test_location_remove_non_existent(runner, temp_dataset_dir):
    """Test `location remove` for a location that doesn't exist."""
    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "NotFound"], input='y\n')

    assert result.exit_code != 0
    assert "Error: Location 'NotFound' does not exist" in result.output

def test_location_remove_main_disallowed(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` attempting to remove 'Main'."""
    # Add another location so 'Main' isn't the only one
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Other": str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "Main"], input='y\n')

    assert result.exit_code != 0
    # Check for either error message, depending on implementation detail
    assert ("Error: Cannot remove the 'Main' location." in result.output or
            "Error: Cannot remove 'Main' location as it's the only one defined" in result.output) 