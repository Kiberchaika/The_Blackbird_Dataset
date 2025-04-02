import pytest
from pathlib import Path
import json
import shutil
from blackbird.dataset import Dataset
from blackbird.schema import DatasetComponentSchema
from blackbird.locations import LocationsManager, SymbolicPathError

EXISTING_TEST_DIR_1 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_2")
EXISTING_TEST_DIR_2 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_3")

@pytest.fixture
def test_dataset(tmp_path):
    """Create a test dataset with various components."""
    dataset_root = tmp_path / "test_dataset"
    
    # Create schema first
    schema = DatasetComponentSchema.create(dataset_root)
    schema.schema["components"].update({
        "instrumental": {
            "pattern": "*_instrumental.mp3",
            "multiple": False
        },
        "vocals": {
            "pattern": "*_vocals_noreverb.mp3",
            "multiple": False
        },
        "mir": {
            "pattern": "*.mir.json",
            "multiple": False
        }
    })
    schema.save()
    
    # Create regular album
    album1 = dataset_root / "Artist1" / "Album1"
    album1.mkdir(parents=True)
    
    # Create files for track1
    (album1 / "track1_instrumental.mp3").touch()
    (album1 / "track1_vocals_noreverb.mp3").touch()
    (album1 / "track1.mir.json").touch()
    
    # Create files for track2 (missing vocals)
    (album1 / "track2_instrumental.mp3").touch()
    (album1 / "track2.mir.json").touch()
    
    # Create CD album
    cd_album = dataset_root / "Artist2" / "Album1"
    (cd_album / "CD1").mkdir(parents=True)
    (cd_album / "CD2").mkdir(parents=True)
    
    # Create files in CD1
    (cd_album / "CD1" / "track1_instrumental.mp3").touch()
    (cd_album / "CD1" / "track1_vocals_noreverb.mp3").touch()
    
    # Create files in CD2
    (cd_album / "CD2" / "track1_instrumental.mp3").touch()
    
    # Add a second location to locations.json for resolver test
    blackbird_dir = dataset_root / ".blackbird"
    locations_path = blackbird_dir / "locations.json"
    locations_data = {
        "Main": str(dataset_root),
        "Backup": str(EXISTING_TEST_DIR_1)
    }
    locations_path.write_text(json.dumps(locations_data))

    # Create structure in the Backup location for resolver test
    (EXISTING_TEST_DIR_1 / "Artist3" / "Album3").mkdir(parents=True, exist_ok=True)
    (EXISTING_TEST_DIR_1 / "Artist3" / "Album3" / "track1_instrumental.mp3").touch()

    yield dataset_root
    
    # Cleanup - remove the test dataset
    shutil.rmtree(dataset_root)

def test_dataset_initialization(test_dataset):
    """Test dataset initialization and index building."""
    dataset = Dataset(test_dataset)
    
    # Verify schema loaded
    assert dataset.schema is not None
    assert "vocals" in dataset.schema.schema["components"]
    assert "mir" in dataset.schema.schema["components"]
    
    # Verify index built
    assert dataset.index is not None
    # Expect tracks from Main (4) + Backup (1 from fixture + files in test_dataset_folder_2)
    # The exact number depends on content of test_dataset_folder_2, check >= 5
    assert len(dataset.index.tracks) >= 5
    # Artists: Artist1, Artist2 from Main, Artist3 from Backup, + artists in test_dataset_folder_2
    assert len(dataset.index.album_by_artist) >= 3
    assert dataset.index.total_size >= 0

def test_find_tracks_all(test_dataset):
    """Test finding all tracks."""
    dataset = Dataset(test_dataset)
    tracks = dataset.find_tracks()
    
    # Should find all tracks from all locations
    assert len(tracks) == len(dataset.index.tracks)
    
    # Verify track paths include location prefixes
    track_paths = set(tracks.keys())
    expected_main_paths = {
        "Main/Artist1/Album1/track1",
        "Main/Artist1/Album1/track2",
        "Main/Artist2/Album1/CD1/track1",
        "Main/Artist2/Album1/CD2/track1"
    }
    expected_backup_paths = {
        "Backup/Artist3/Album3/track1" # Added in fixture
        # Potentially more paths from test_dataset_folder_2
    }
    assert expected_main_paths.issubset(track_paths)
    assert any(p.startswith("Backup/") for p in track_paths)

def test_find_tracks_with_components(test_dataset):
    """Test finding tracks with specific components."""
    dataset = Dataset(test_dataset)
    
    # Find tracks with vocals (check components across locations)
    tracks_with_vocals = dataset.find_tracks(has=["vocals"])
    # Expected: Main/Artist1/Album1/track1, Main/Artist2/Album1/CD1/track1
    # Plus any in Backup location
    assert len(tracks_with_vocals) >= 2
    assert "Main/Artist1/Album1/track1" in tracks_with_vocals
    assert "Main/Artist2/Album1/CD1/track1" in tracks_with_vocals
    
    # Find tracks with both vocals and mir
    complete_tracks = dataset.find_tracks(has=["vocals", "mir"])
    # Expected: Main/Artist1/Album1/track1 (assuming Backup doesn't have complete tracks)
    assert len(complete_tracks) >= 1
    assert "Main/Artist1/Album1/track1" in complete_tracks

def test_find_tracks_missing_components(test_dataset):
    """Test finding tracks missing components."""
    dataset = Dataset(test_dataset)
    
    # Find tracks missing vocals
    tracks_no_vocals = dataset.find_tracks(missing=["vocals"])
    # Expected: Main/Artist1/Album1/track2, Main/Artist2/Album1/CD2/track1, Backup/Artist3/Album3/track1
    # Plus any in Backup location missing vocals
    assert len(tracks_no_vocals) >= 3
    assert "Main/Artist1/Album1/track2" in tracks_no_vocals
    assert "Main/Artist2/Album1/CD2/track1" in tracks_no_vocals
    assert "Backup/Artist3/Album3/track1" in tracks_no_vocals

def test_find_tracks_by_artist(test_dataset):
    """Test finding tracks filtered by artist."""
    dataset = Dataset(test_dataset)
    
    # Find Artist1's tracks (only in Main location in fixture)
    artist1_tracks = dataset.find_tracks(artist="Artist1")
    assert len(artist1_tracks) == 2
    assert all(t.startswith("Main/Artist1/") for t in artist1_tracks.keys())
    
    # Find Artist2's tracks (only in Main location in fixture)
    artist2_tracks = dataset.find_tracks(artist="Artist2")
    assert len(artist2_tracks) == 2
    assert all(t.startswith("Main/Artist2/") for t in artist2_tracks.keys())
    
    # Find Artist3's tracks (only in Backup location in fixture)
    artist3_tracks = dataset.find_tracks(artist="Artist3")
    assert len(artist3_tracks) >= 1 # Could be more if Artist3 exists in test_dataset_folder_2
    assert "Backup/Artist3/Album3/track1" in artist3_tracks

def test_analyze_dataset(test_dataset):
    """Test dataset analysis."""
    dataset = Dataset(test_dataset)
    stats = dataset.analyze()
    
    assert stats["tracks"]["total"] == len(dataset.index.tracks)
    assert len(stats["artists"]) == len(dataset.index.album_by_artist)
    # Counts depend on Backup location content, check minimums from fixture
    assert stats["components"]["instrumental"]["count"] >= 5
    assert stats["components"]["vocals"]["count"] >= 2
    assert stats["components"]["mir"]["count"] >= 2
    
    # Check artist-specific stats (minimums)
    assert stats["tracks"]["by_artist"].get("Artist1", 0) == 2
    assert stats["tracks"]["by_artist"].get("Artist2", 0) == 2
    assert stats["tracks"]["by_artist"].get("Artist3", 0) >= 1
    
    # Check album structure
    assert "Album1" in stats["albums"]["Artist1"]
    assert "Album1" in stats["albums"]["Artist2"]
    assert "Album3" in stats["albums"].get("Artist3", set())

def test_rebuild_index(test_dataset):
    """Test manual index rebuilding."""
    dataset = Dataset(test_dataset)
    initial_index = dataset.index
    
    # Add a new file to Main location
    (test_dataset / "Artist1" / "Album1" / "track3_instrumental.mp3").touch()
    
    # Rebuild index
    dataset.rebuild_index()
    
    # Verify index was updated
    assert len(dataset.index.tracks) == len(initial_index.tracks) + 1
    assert "Main/Artist1/Album1/track3" in dataset.index.tracks # Check symbolic path

# --- Tests for Dataset.resolve_path ---

def test_dataset_resolve_path_valid(test_dataset):
    """Test Dataset.resolve_path with a valid symbolic path."""
    dataset = Dataset(test_dataset)
    # Assuming 'Main' location points to test_dataset path
    expected_path = test_dataset / "Artist1/Album1/track1.mp3"
    resolved_path = dataset.resolve_path("Main/Artist1/Album1/track1.mp3")
    assert resolved_path == expected_path.resolve()

def test_dataset_resolve_path_unknown_location(test_dataset):
    """Test Dataset.resolve_path with an unknown location."""
    dataset = Dataset(test_dataset)
    with pytest.raises(SymbolicPathError, match="Unknown location name 'UnknownLoc'"):
        dataset.resolve_path("UnknownLoc/some/path")

def test_dataset_resolve_path_invalid_format(test_dataset):
    """Test Dataset.resolve_path with invalid symbolic path format."""
    dataset = Dataset(test_dataset)
    # Test 1: Invalid format (No separator, not a location name)
    with pytest.raises(SymbolicPathError, match="Invalid symbolic path format: 'NoSeparatorHere'"):
        dataset.resolve_path("NoSeparatorHere")
    # Test 2: Invalid format (Leading Slash)
    with pytest.raises(SymbolicPathError, match="empty location name part"):
        dataset.resolve_path("/LeadingSlash/path")

def test_dataset_resolve_path_after_location_change(test_dataset):
    """Test path resolution after adding/removing locations."""
    dataset = Dataset(test_dataset)

    # Initially resolve a path in Backup
    symbolic_backup = "Backup/Artist3/Album3/track1_instrumental.mp3"
    expected_backup = EXISTING_TEST_DIR_1 / "Artist3" / "Album3" / "track1_instrumental.mp3"
    assert dataset.resolve_path(symbolic_backup) == expected_backup

    # Programmatically remove the Backup location (using LocationsManager)
    dataset.locations.remove_location("Backup")
    # NOTE: Dataset does not automatically reload locations on change.
    # Re-initializing or manually reloading would be needed for a real scenario.
    # For this test, we check that resolve_path *still uses the old loaded locations*
    # This highlights that resolve_path uses the locations loaded at Dataset init time.
    # If dynamic location reloading is desired, Dataset logic would need adjustment.

    # Trying to resolve the same path should now fail because 'Backup' is gone from the *instance's* locations
    # Re-create dataset instance to reload locations
    dataset_reloaded = Dataset(test_dataset)
    dataset_reloaded.locations.load_locations() # Force reload
    dataset_reloaded.locations.remove_location("Backup") # Now remove it from the reloaded instance

    with pytest.raises(SymbolicPathError, match="Unknown location name 'Backup'"):
         dataset_reloaded.resolve_path(symbolic_backup)

    # Add a new location and test resolution
    dataset_reloaded.locations.add_location("NewLoc", str(EXISTING_TEST_DIR_2))
    # We need to create a dummy file there for the path to be meaningful
    (EXISTING_TEST_DIR_2 / "new_file.txt").touch()
    symbolic_new = "NewLoc/new_file.txt"
    expected_new = EXISTING_TEST_DIR_2 / "new_file.txt"
    assert dataset_reloaded.resolve_path(symbolic_new) == expected_new 