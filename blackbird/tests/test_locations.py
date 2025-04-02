import pytest
import json
from pathlib import Path
from blackbird.locations import LocationsManager

# Use specific paths mentioned in the plan for testing existence
EXISTING_TEST_DIR_1 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_2")
EXISTING_TEST_DIR_2 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_3")
NON_EXISTENT_DIR = Path("/tmp/non_existent_blackbird_test_dir")

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
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)
    return dataset_root

@pytest.fixture
def locations_json_path(temp_dataset_dir):
    """Path to the locations.json file within the temp dataset dir."""
    return temp_dataset_dir / ".blackbird" / "locations.json"

@pytest.fixture
def locations_manager(temp_dataset_dir):
    """Provides a LocationsManager instance initialized with the temp dir."""
    return LocationsManager(temp_dataset_dir)

def test_load_default_location_when_json_absent(locations_manager, temp_dataset_dir, locations_json_path):
    """Test loading default 'Main' location if locations.json doesn't exist."""
    assert not locations_json_path.exists()
    locations_manager.load_locations()
    assert locations_manager.get_all_locations() == {"Main": str(temp_dataset_dir)}
    assert locations_manager.get_location_path("Main") == temp_dataset_dir

def test_load_existing_locations_json(locations_manager, locations_json_path, temp_dataset_dir):
    """Test loading locations from an existing locations.json file."""
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Backup": str(EXISTING_TEST_DIR_1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    locations_manager.load_locations()
    assert locations_manager.get_all_locations() == loc_data
    assert locations_manager.get_location_path("Main") == temp_dataset_dir
    assert locations_manager.get_location_path("Backup") == EXISTING_TEST_DIR_1

def test_save_locations(locations_manager, locations_json_path, temp_dataset_dir):
    """Test saving locations to locations.json."""
    loc_data = {
        "Main": str(temp_dataset_dir),
        "SSD_Fast": str(EXISTING_TEST_DIR_2)
    }
    locations_manager._locations = loc_data # Directly set internal state for saving
    locations_manager.save_locations()

    assert locations_json_path.exists()
    loaded_data = json.loads(locations_json_path.read_text())
    assert loaded_data == loc_data

def test_add_location_success(locations_manager, temp_dataset_dir):
    """Test adding a valid new location."""
    locations_manager.load_locations() # Load default "Main"
    assert locations_manager.add_location("SSD_Fast", str(EXISTING_TEST_DIR_1))
    assert "SSD_Fast" in locations_manager.get_all_locations()
    assert locations_manager.get_location_path("SSD_Fast") == EXISTING_TEST_DIR_1
    assert locations_manager.get_all_locations() == {
        "Main": str(temp_dataset_dir),
        "SSD_Fast": str(EXISTING_TEST_DIR_1)
    }

def test_add_location_duplicate_name(locations_manager, temp_dataset_dir):
    """Test adding a location with a name that already exists."""
    locations_manager.load_locations()
    with pytest.raises(ValueError, match="Location 'Main' already exists"):
        locations_manager.add_location("Main", str(EXISTING_TEST_DIR_1))

def test_add_location_invalid_path_non_existent(locations_manager):
    """Test adding a location with a path that does not exist."""
    locations_manager.load_locations()
    with pytest.raises(ValueError, match=f"Path does not exist or is not a directory: {NON_EXISTENT_DIR}"):
        locations_manager.add_location("Invalid", str(NON_EXISTENT_DIR))

def test_add_location_invalid_path_is_file(locations_manager, tmp_path):
    """Test adding a location with a path that points to a file."""
    file_path = tmp_path / "test_file.txt"
    file_path.touch()
    locations_manager.load_locations()
    with pytest.raises(ValueError, match=f"Path does not exist or is not a directory: {file_path}"):
        locations_manager.add_location("InvalidFile", str(file_path))

def test_remove_location_success(locations_manager, temp_dataset_dir):
    """Test removing an existing location (other than 'Main')."""
    locations_manager.load_locations()
    locations_manager.add_location("ToDelete", str(EXISTING_TEST_DIR_1))
    assert "ToDelete" in locations_manager.get_all_locations()

    assert locations_manager.remove_location("ToDelete")
    assert "ToDelete" not in locations_manager.get_all_locations()
    assert locations_manager.get_all_locations() == {"Main": str(temp_dataset_dir)}

def test_remove_location_non_existent(locations_manager):
    """Test removing a location that does not exist."""
    locations_manager.load_locations()
    with pytest.raises(ValueError, match="Location 'NotFound' does not exist"):
        locations_manager.remove_location("NotFound")

def test_remove_location_main_when_only_one(locations_manager):
    """Test attempting to remove 'Main' when it's the only location."""
    locations_manager.load_locations()
    with pytest.raises(ValueError, match="Cannot remove 'Main' location as it's the only one defined"):
        locations_manager.remove_location("Main")

def test_remove_location_main_when_multiple_exist(locations_manager):
    """Test removing 'Main' is disallowed even if other locations exist (current rule)."""
    # Note: The plan mentions disallowing removal if it's the only one OR if target for move exists.
    # The current implementation seems to disallow removing 'Main' unconditionally.
    # This test reflects the current strict behavior. If the rule changes (e.g., allow removal if others exist),
    # this test should be updated.
    locations_manager.load_locations()
    locations_manager.add_location("Other", str(EXISTING_TEST_DIR_1))
    with pytest.raises(ValueError, match="Cannot remove the 'Main' location"):
         locations_manager.remove_location("Main")


def test_get_location_path_success(locations_manager, temp_dataset_dir):
    """Test getting the path for an existing location."""
    locations_manager.load_locations()
    locations_manager.add_location("Backup", str(EXISTING_TEST_DIR_1))
    assert locations_manager.get_location_path("Main") == temp_dataset_dir
    assert locations_manager.get_location_path("Backup") == EXISTING_TEST_DIR_1

def test_get_location_path_non_existent(locations_manager):
    """Test getting the path for a non-existent location."""
    locations_manager.load_locations()
    with pytest.raises(KeyError, match="'NotFound'"):
        locations_manager.get_location_path("NotFound")

def test_get_all_locations(locations_manager, temp_dataset_dir):
    """Test retrieving all defined locations."""
    locations_manager.load_locations()
    locations_manager.add_location("Backup", str(EXISTING_TEST_DIR_1))
    locations_manager.add_location("SSD_Fast", str(EXISTING_TEST_DIR_2))
    expected = {
        "Main": str(temp_dataset_dir),
        "Backup": str(EXISTING_TEST_DIR_1),
        "SSD_Fast": str(EXISTING_TEST_DIR_2)
    }
    assert locations_manager.get_all_locations() == expected

def test_locations_json_persistence(temp_dataset_dir, locations_json_path):
    """Test that changes persist across instances via the JSON file."""
    # Instance 1: Add location and save
    lm1 = LocationsManager(temp_dataset_dir)
    lm1.load_locations()
    lm1.add_location("External", str(EXISTING_TEST_DIR_1))
    lm1.save_locations()

    assert locations_json_path.exists()

    # Instance 2: Load and verify
    lm2 = LocationsManager(temp_dataset_dir)
    lm2.load_locations()
    expected = {
        "Main": str(temp_dataset_dir),
        "External": str(EXISTING_TEST_DIR_1)
    }
    assert lm2.get_all_locations() == expected

    # Instance 2: Remove location and save
    lm2.remove_location("External")
    lm2.save_locations()

    # Instance 3: Load and verify removal
    lm3 = LocationsManager(temp_dataset_dir)
    lm3.load_locations()
    expected_after_remove = {
        "Main": str(temp_dataset_dir)
    }
    assert lm3.get_all_locations() == expected_after_remove 