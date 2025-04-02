import pytest
import json
import shutil
from pathlib import Path
from typing import Dict

# Assuming blackbird and its submodules are importable
from blackbird.locations import LocationsManager, resolve_symbolic_path, LocationValidationError, SymbolicPathError # Import exceptions
# Fixtures from conftest.py are automatically available

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
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    return dataset_dir

@pytest.fixture
def locations_json_path(temp_dataset_dir):
    """Path to the locations.json file within the temp dataset dir."""
    return temp_dataset_dir / ".blackbird" / "locations.json"

@pytest.fixture
def locations_manager(temp_dataset_dir):
    """Provides a LocationsManager instance initialized with the temp dir."""
    # Ensure .blackbird dir exists for the manager
    (temp_dataset_dir / ".blackbird").mkdir(exist_ok=True)
    return LocationsManager(temp_dataset_dir)

# Fixture for sample locations used in resolve_symbolic_path tests
@pytest.fixture
def sample_locations_real_paths(tmp_path):
    """Creates temporary directories for testing resolve_symbolic_path"""
    base = tmp_path / "resolve_path_test_dirs"
    locs = {
        "Main": base / "main_data",
        "Archive": base / "archive" / "store",
        "SSD_Fast": base / "ssd_data",
        "Spaced Loc": base / "path with spaces" / "data"
    }
    for p in locs.values():
        p.mkdir(parents=True, exist_ok=True)
    # Return dict mapping name to the created Path object
    return locs

def test_load_default_location_when_json_absent(locations_manager, temp_dataset_dir, locations_json_path):
    """Test loading default 'Main' location if locations.json doesn't exist."""
    assert not locations_json_path.exists()
    locations_manager.load_locations()
    # Expect Path objects now
    assert locations_manager.get_all_locations() == {"Main": temp_dataset_dir.resolve()}

def test_load_existing_locations_json(locations_manager, locations_json_path, temp_dataset_dir):
    """Test loading locations from an existing locations.json file."""
    # Ensure target dirs for locations exist for validation during load
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True)
    loc_data = {
        "Main": str(temp_dataset_dir.resolve()),
        "Backup": str(EXISTING_TEST_DIR_1.resolve())
    }
    locations_json_path.parent.mkdir(exist_ok=True) # Ensure .blackbird exists
    locations_json_path.write_text(json.dumps(loc_data))

    locations_manager.load_locations()
    # Expect Path objects, compare resolved paths
    expected_paths = {name: Path(p).resolve() for name, p in loc_data.items()}
    assert locations_manager.get_all_locations() == expected_paths

def test_save_locations(locations_manager, locations_json_path, temp_dataset_dir):
    """Test saving the current locations to locations.json."""
    # Ensure target dirs exist
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True)
    EXISTING_TEST_DIR_2.mkdir(parents=True, exist_ok=True)

    locations_manager.load_locations() # Loads Main
    locations_manager.add_location("Backup", str(EXISTING_TEST_DIR_1))
    locations_manager.add_location("SSD", str(EXISTING_TEST_DIR_2))
    locations_manager.save_locations()

    assert locations_json_path.exists()
    saved_data = json.loads(locations_json_path.read_text())
    # Compare saved strings with resolved paths from manager
    expected_saved = {
        "Main": str(temp_dataset_dir.resolve()),
        "Backup": str(EXISTING_TEST_DIR_1.resolve()),
        "SSD": str(EXISTING_TEST_DIR_2.resolve())
    }
    assert saved_data == expected_saved

def test_add_location_success(locations_manager, temp_dataset_dir):
    """Test adding a valid new location."""
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True) # Ensure path exists
    locations_manager.load_locations() # Load default "Main"
    # Method doesn't return bool, check side effect
    locations_manager.add_location("SSD_Fast", str(EXISTING_TEST_DIR_1))
    all_locs = locations_manager.get_all_locations()
    assert "SSD_Fast" in all_locs
    assert all_locs["SSD_Fast"] == EXISTING_TEST_DIR_1.resolve()
    assert "Main" in all_locs # Ensure Main is still there

def test_add_location_duplicate_name(locations_manager, temp_dataset_dir):
    """Test adding a location with a name that already exists."""
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True) # Ensure path exists
    locations_manager.load_locations()
    with pytest.raises(LocationValidationError, match="Location name 'Main' already exists"):
        # Use a valid path, the error is the duplicate name
        locations_manager.add_location("Main", str(EXISTING_TEST_DIR_1))

def test_add_location_invalid_path_non_existent(locations_manager):
    """Test adding a location with a path that does not exist."""
    locations_manager.load_locations()
    assert not NON_EXISTENT_DIR.exists() # Precondition
    with pytest.raises(LocationValidationError, match=f"Path '{NON_EXISTENT_DIR}' does not exist"):
        locations_manager.add_location("WontWork", str(NON_EXISTENT_DIR))

def test_add_location_invalid_path_is_file(locations_manager, tmp_path):
    """Test adding a location with a path that points to a file."""
    file_path = tmp_path / "test_file.txt"
    file_path.touch() # Create the file
    locations_manager.load_locations()
    with pytest.raises(LocationValidationError, match=f"Path '{file_path.resolve()}' exists but is not a directory"):
        locations_manager.add_location("NotADir", str(file_path))

def test_remove_location_success(locations_manager, temp_dataset_dir):
    """Test removing an existing location (other than 'Main')."""
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True) # Ensure path exists
    locations_manager.load_locations()
    locations_manager.add_location("ToDelete", str(EXISTING_TEST_DIR_1))
    assert "ToDelete" in locations_manager.get_all_locations()

    # Method doesn't return bool, check side effect
    locations_manager.remove_location("ToDelete")
    assert "ToDelete" not in locations_manager.get_all_locations()
    assert "Main" in locations_manager.get_all_locations() # Ensure Main is still there

def test_remove_location_non_existent(locations_manager):
    """Test removing a location that does not exist."""
    locations_manager.load_locations()
    with pytest.raises(LocationValidationError, match="Location 'NotFound' does not exist"):
        locations_manager.remove_location("NotFound")

def test_remove_location_main_when_only_one(locations_manager):
    """Test attempting to remove 'Main' when it's the only location."""
    locations_manager.load_locations()
    assert list(locations_manager.get_all_locations().keys()) == ["Main"]
    with pytest.raises(LocationValidationError, match="Cannot remove the default location 'Main' when it is the only location"):
        locations_manager.remove_location("Main")

def test_remove_location_main_when_multiple_exist(locations_manager):
    """Test removing 'Main' is allowed if other locations exist."""
    # Current implementation allows removing 'Main'. This test verifies that behavior.
    # If the requirement changes to *prevent* removing Main, this test needs modification.
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True)
    locations_manager.load_locations()
    locations_manager.add_location("Other", str(EXISTING_TEST_DIR_1))
    assert "Main" in locations_manager.get_all_locations()
    assert "Other" in locations_manager.get_all_locations()

    # Check side effect: Main should be gone
    locations_manager.remove_location("Main")
    assert "Main" not in locations_manager.get_all_locations()
    assert "Other" in locations_manager.get_all_locations() # Other should remain


def test_get_location_path_success(locations_manager, temp_dataset_dir):
    """Test getting the path for an existing location."""
    locations_manager.load_locations()
    assert locations_manager.get_location_path("Main") == temp_dataset_dir.resolve()

def test_get_location_path_non_existent(locations_manager):
    """Test getting the path for a non-existent location."""
    locations_manager.load_locations()
    # Method raises KeyError as per implementation
    with pytest.raises(KeyError, match="Location 'NotFound' not found"):
        locations_manager.get_location_path("NotFound")


def test_get_all_locations(locations_manager, temp_dataset_dir):
    """Test retrieving all defined locations."""
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True)
    EXISTING_TEST_DIR_2.mkdir(parents=True, exist_ok=True)
    locations_manager.load_locations()
    locations_manager.add_location("Backup", str(EXISTING_TEST_DIR_1))
    locations_manager.add_location("SSD_Fast", str(EXISTING_TEST_DIR_2))

    # Expect Path objects
    expected = {
        "Main": temp_dataset_dir.resolve(),
        "Backup": EXISTING_TEST_DIR_1.resolve(),
        "SSD_Fast": EXISTING_TEST_DIR_2.resolve()
    }
    assert locations_manager.get_all_locations() == expected

def test_locations_json_persistence(temp_dataset_dir, locations_json_path):
    """Test that changes persist across instances via the JSON file."""
    EXISTING_TEST_DIR_1.mkdir(parents=True, exist_ok=True) # Ensure path exists

    # Instance 1: Add location and save
    lm1 = LocationsManager(temp_dataset_dir)
    lm1.load_locations()
    lm1.add_location("External", str(EXISTING_TEST_DIR_1))
    lm1.save_locations()

    assert locations_json_path.exists()

    # Instance 2: Load and verify
    lm2 = LocationsManager(temp_dataset_dir)
    lm2.load_locations()
    # Expect Path objects
    expected = {
        "Main": temp_dataset_dir.resolve(),
        "External": EXISTING_TEST_DIR_1.resolve()
    }
    assert lm2.get_all_locations() == expected

# == Symbolic Path Resolution Tests (using standalone function) ==

# Use the fixture `sample_locations_real_paths` which creates temp dirs
def test_resolve_symbolic_path_valid(sample_locations_real_paths):
    """Test resolving valid symbolic paths using temporary directories."""
    # sample_locations_real_paths provides Paths to existing temp dirs
    assert resolve_symbolic_path("Main/Artist/Album/track.mp3", sample_locations_real_paths) == \
           sample_locations_real_paths["Main"] / "Artist/Album/track.mp3"

    assert resolve_symbolic_path("Archive/subdir/file.txt", sample_locations_real_paths) == \
           sample_locations_real_paths["Archive"] / "subdir/file.txt"

    # Test resolving location root
    assert resolve_symbolic_path("SSD_Fast", sample_locations_real_paths) == \
           sample_locations_real_paths["SSD_Fast"]


def test_resolve_symbolic_path_invalid_format(sample_locations_real_paths):
    """Test resolving paths with invalid formats."""
    # A path without a separator is an invalid format unless it's a location name
    with pytest.raises(SymbolicPathError, match="Invalid symbolic path format: 'MissingSeparator'"):
         resolve_symbolic_path("MissingSeparator", sample_locations_real_paths)

    with pytest.raises(SymbolicPathError, match="empty location name part"):
         resolve_symbolic_path("/LeadingSlash/path", sample_locations_real_paths)

    # Path: "Location//DoubleSlash/path"
    # Problem: 'Location' is not a known location name.
    with pytest.raises(SymbolicPathError, match="Unknown location name 'Location'"):
       resolve_symbolic_path("Location//DoubleSlash/path", sample_locations_real_paths)

    # Path: "Main/" (empty relative part after slash)
    with pytest.raises(SymbolicPathError, match="invalid or directory-like relative path part"):
        resolve_symbolic_path("Main/", sample_locations_real_paths)


def test_resolve_symbolic_path_unknown_location(sample_locations_real_paths):
    """Test resolving a path with an unknown location name."""
    with pytest.raises(SymbolicPathError, match="Unknown location name 'UnknownLoc'"):
        resolve_symbolic_path("UnknownLoc/some/path.file", sample_locations_real_paths)

def test_resolve_symbolic_path_with_empty_locations():
    """Test resolving path with an empty locations dictionary."""
    with pytest.raises(ValueError, match="Locations must be a non-empty dictionary"):
        resolve_symbolic_path("Main/path", {})

def test_resolve_symbolic_path_with_spaces(sample_locations_real_paths):
    """Test resolving paths containing spaces using temporary directories."""
    # sample_locations_real_paths includes "Spaced Loc" pointing to a temp dir
    assert resolve_symbolic_path("Spaced Loc/Artist Name/Album Title/track name.mp3", sample_locations_real_paths) == \
           sample_locations_real_paths["Spaced Loc"] / "Artist Name/Album Title/track name.mp3" 