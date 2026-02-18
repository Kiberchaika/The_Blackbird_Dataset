import pytest
import json
from pathlib import Path

from blackbird.locations import LocationsManager, resolve_symbolic_path, LocationValidationError, SymbolicPathError


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
    (temp_dataset_dir / ".blackbird").mkdir(exist_ok=True)
    return LocationsManager(temp_dataset_dir)


@pytest.fixture
def extra_dirs(tmp_path):
    """Creates two extra temporary directories for use as additional locations."""
    dir1 = tmp_path / "external_dir_1"
    dir2 = tmp_path / "external_dir_2"
    dir1.mkdir()
    dir2.mkdir()
    return dir1, dir2


@pytest.fixture
def sample_locations_real_paths(tmp_path):
    """Creates temporary directories for testing resolve_symbolic_path."""
    base = tmp_path / "resolve_path_test_dirs"
    locs = {
        "Main": base / "main_data",
        "Archive": base / "archive" / "store",
        "SSD_Fast": base / "ssd_data",
        "Spaced Loc": base / "path with spaces" / "data"
    }
    for p in locs.values():
        p.mkdir(parents=True, exist_ok=True)
    return locs


def test_load_default_location_when_json_absent(locations_manager, temp_dataset_dir, locations_json_path):
    """Test loading default 'Main' location if locations.json doesn't exist."""
    assert not locations_json_path.exists()
    locations_manager.load_locations()
    assert locations_manager.get_all_locations() == {"Main": temp_dataset_dir.resolve()}


def test_load_existing_locations_json(locations_manager, locations_json_path, temp_dataset_dir, extra_dirs):
    """Test loading locations from an existing locations.json file."""
    dir1, _ = extra_dirs
    loc_data = {
        "Main": str(temp_dataset_dir.resolve()),
        "Backup": str(dir1.resolve())
    }
    locations_json_path.parent.mkdir(exist_ok=True)
    locations_json_path.write_text(json.dumps(loc_data))

    locations_manager.load_locations()
    expected_paths = {name: Path(p).resolve() for name, p in loc_data.items()}
    assert locations_manager.get_all_locations() == expected_paths


def test_save_locations(locations_manager, locations_json_path, temp_dataset_dir, extra_dirs):
    """Test saving the current locations to locations.json."""
    dir1, dir2 = extra_dirs

    locations_manager.load_locations()
    locations_manager.add_location("Backup", str(dir1))
    locations_manager.add_location("SSD", str(dir2))
    locations_manager.save_locations()

    assert locations_json_path.exists()
    saved_data = json.loads(locations_json_path.read_text())
    expected_saved = {
        "Main": str(temp_dataset_dir.resolve()),
        "Backup": str(dir1.resolve()),
        "SSD": str(dir2.resolve())
    }
    assert saved_data == expected_saved


def test_add_location_success(locations_manager, temp_dataset_dir, extra_dirs):
    """Test adding a valid new location."""
    dir1, _ = extra_dirs
    locations_manager.load_locations()
    locations_manager.add_location("SSD_Fast", str(dir1))
    all_locs = locations_manager.get_all_locations()
    assert "SSD_Fast" in all_locs
    assert all_locs["SSD_Fast"] == dir1.resolve()
    assert "Main" in all_locs


def test_add_location_duplicate_name(locations_manager, temp_dataset_dir, extra_dirs):
    """Test adding a location with a name that already exists."""
    dir1, _ = extra_dirs
    locations_manager.load_locations()
    with pytest.raises(LocationValidationError, match="Location name 'Main' already exists"):
        locations_manager.add_location("Main", str(dir1))


def test_add_location_invalid_path_non_existent(locations_manager, tmp_path):
    """Test adding a location with a path that does not exist."""
    non_existent = tmp_path / "non_existent_dir"
    locations_manager.load_locations()
    assert not non_existent.exists()
    with pytest.raises(LocationValidationError, match=f"Path '{non_existent}' does not exist"):
        locations_manager.add_location("WontWork", str(non_existent))


def test_add_location_invalid_path_is_file(locations_manager, tmp_path):
    """Test adding a location with a path that points to a file."""
    file_path = tmp_path / "test_file.txt"
    file_path.touch()
    locations_manager.load_locations()
    with pytest.raises(LocationValidationError, match=f"Path '{file_path.resolve()}' exists but is not a directory"):
        locations_manager.add_location("NotADir", str(file_path))


def test_remove_location_success(locations_manager, temp_dataset_dir, extra_dirs):
    """Test removing an existing location (other than 'Main')."""
    dir1, _ = extra_dirs
    locations_manager.load_locations()
    locations_manager.add_location("ToDelete", str(dir1))
    assert "ToDelete" in locations_manager.get_all_locations()

    locations_manager.remove_location("ToDelete")
    assert "ToDelete" not in locations_manager.get_all_locations()
    assert "Main" in locations_manager.get_all_locations()


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


def test_remove_location_main_when_multiple_exist(locations_manager, extra_dirs):
    """Test removing 'Main' is allowed if other locations exist."""
    dir1, _ = extra_dirs
    locations_manager.load_locations()
    locations_manager.add_location("Other", str(dir1))
    assert "Main" in locations_manager.get_all_locations()
    assert "Other" in locations_manager.get_all_locations()

    locations_manager.remove_location("Main")
    assert "Main" not in locations_manager.get_all_locations()
    assert "Other" in locations_manager.get_all_locations()


def test_get_location_path_success(locations_manager, temp_dataset_dir):
    """Test getting the path for an existing location."""
    locations_manager.load_locations()
    assert locations_manager.get_location_path("Main") == temp_dataset_dir.resolve()


def test_get_location_path_non_existent(locations_manager):
    """Test getting the path for a non-existent location."""
    locations_manager.load_locations()
    with pytest.raises(KeyError, match="Location 'NotFound' not found"):
        locations_manager.get_location_path("NotFound")


def test_get_all_locations(locations_manager, temp_dataset_dir, extra_dirs):
    """Test retrieving all defined locations."""
    dir1, dir2 = extra_dirs
    locations_manager.load_locations()
    locations_manager.add_location("Backup", str(dir1))
    locations_manager.add_location("SSD_Fast", str(dir2))

    expected = {
        "Main": temp_dataset_dir.resolve(),
        "Backup": dir1.resolve(),
        "SSD_Fast": dir2.resolve()
    }
    assert locations_manager.get_all_locations() == expected


def test_locations_json_persistence(temp_dataset_dir, locations_json_path, extra_dirs):
    """Test that changes persist across instances via the JSON file."""
    dir1, _ = extra_dirs

    # Instance 1: Add location and save
    lm1 = LocationsManager(temp_dataset_dir)
    lm1.load_locations()
    lm1.add_location("External", str(dir1))
    lm1.save_locations()

    assert locations_json_path.exists()

    # Instance 2: Load and verify
    lm2 = LocationsManager(temp_dataset_dir)
    lm2.load_locations()
    expected = {
        "Main": temp_dataset_dir.resolve(),
        "External": dir1.resolve()
    }
    assert lm2.get_all_locations() == expected


# == Symbolic Path Resolution Tests (using standalone function) ==

def test_resolve_symbolic_path_valid(sample_locations_real_paths):
    """Test resolving valid symbolic paths using temporary directories."""
    assert resolve_symbolic_path("Main/Artist/Album/track.mp3", sample_locations_real_paths) == \
           sample_locations_real_paths["Main"] / "Artist/Album/track.mp3"

    assert resolve_symbolic_path("Archive/subdir/file.txt", sample_locations_real_paths) == \
           sample_locations_real_paths["Archive"] / "subdir/file.txt"

    assert resolve_symbolic_path("SSD_Fast", sample_locations_real_paths) == \
           sample_locations_real_paths["SSD_Fast"]


def test_resolve_symbolic_path_invalid_format(sample_locations_real_paths):
    """Test resolving paths with invalid formats."""
    with pytest.raises(SymbolicPathError, match="Invalid symbolic path format: 'MissingSeparator'"):
         resolve_symbolic_path("MissingSeparator", sample_locations_real_paths)

    with pytest.raises(SymbolicPathError, match="empty location name part"):
         resolve_symbolic_path("/LeadingSlash/path", sample_locations_real_paths)

    with pytest.raises(SymbolicPathError, match="Unknown location name 'Location'"):
       resolve_symbolic_path("Location//DoubleSlash/path", sample_locations_real_paths)

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
    assert resolve_symbolic_path("Spaced Loc/Artist Name/Album Title/track name.mp3", sample_locations_real_paths) == \
           sample_locations_real_paths["Spaced Loc"] / "Artist Name/Album Title/track name.mp3"
