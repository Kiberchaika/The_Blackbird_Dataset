import pytest
from pathlib import Path
from datetime import datetime
from blackbird.index import DatasetIndex, TrackInfo
import json

# Define specific paths for testing
EXISTING_TEST_DIR_1 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_2")
EXISTING_TEST_DIR_2 = Path("/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_3")

@pytest.fixture
def multi_location_dataset(tmp_path):
    """Sets up a temporary dataset structure with multiple locations."""
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)

    # Create locations.json
    locations_data = {
        "Main": str(dataset_root),
        "Loc2": str(EXISTING_TEST_DIR_1),
        "Loc3": str(EXISTING_TEST_DIR_2) # Using another existing dir for diversity
    }
    locations_json_path = blackbird_dir / "locations.json"
    locations_json_path.write_text(json.dumps(locations_data))

    # Create dummy schema (needed for index building)
    schema_path = blackbird_dir / "schema.json"
    schema_data = {
        "version": "1.0",
        "components": {
            "instrumental_audio": {
                "pattern": "*_instrumental.wav",
                "multiple": False,
                "description": ""
            }
        }
    }
    schema_path.write_text(json.dumps(schema_data))

    # Create some structure and files in 'Main' location
    (dataset_root / "Artist1" / "Album1").mkdir(parents=True)
    (dataset_root / "Artist1" / "Album1" / "Track1_instrumental.wav").touch()

    # Ensure structure exists in external locations (the fixture for locations handles dir creation)
    # We assume EXISTING_TEST_DIR_1 and _2 have some structure for real tests,
    # but for the index structure itself, we only need the paths defined.
    (EXISTING_TEST_DIR_1 / "Artist1" / "Album2").mkdir(parents=True, exist_ok=True)
    (EXISTING_TEST_DIR_1 / "Artist1" / "Album2" / "Track2_instrumental.wav").touch()
    (EXISTING_TEST_DIR_2 / "Artist2" / "Album3").mkdir(parents=True, exist_ok=True)
    (EXISTING_TEST_DIR_2 / "Artist2" / "Album3" / "Track3_instrumental.wav").touch()


    return dataset_root, locations_data

@pytest.fixture
def sample_index_symbolic(multi_location_dataset):
    """Creates a sample index with symbolic paths spanning multiple locations."""
    dataset_root, locations_data = multi_location_dataset
    index = DatasetIndex.create()
    index.stats_by_location = {"Main": {}, "Loc2": {}, "Loc3":{}} # Initialize

    # Add sample data using symbolic paths
    track1 = TrackInfo(
        track_path="Main/Artist1/Album1/Track1",
        artist="Artist1",
        album_path="Main/Artist1/Album1",
        cd_number=None,
        base_name="Track1",
        files={"instrumental_audio": "Main/Artist1/Album1/Track1_instrumental.wav"},
        file_sizes={"Main/Artist1/Album1/Track1_instrumental.wav": 1000}
    )

    track2 = TrackInfo(
        track_path="Loc2/Artist1/Album2/Track2",
        artist="Artist1",
        album_path="Loc2/Artist1/Album2",
        cd_number=None,
        base_name="Track2",
        files={"instrumental_audio": "Loc2/Artist1/Album2/Track2_instrumental.wav"},
        file_sizes={"Loc2/Artist1/Album2/Track2_instrumental.wav": 1500} # Different size
    )

    track3 = TrackInfo(
        track_path="Loc3/Artist2/Album3/Track3",
        artist="Artist2",
        album_path="Loc3/Artist2/Album3",
        cd_number=None,
        base_name="Track3",
        files={"instrumental_audio": "Loc3/Artist2/Album3/Track3_instrumental.wav"},
        file_sizes={"Loc3/Artist2/Album3/Track3_instrumental.wav": 2000} # Different size
    )

    # Add tracks to index
    for track in [track1, track2, track3]:
        index.tracks[track.track_path] = track
        index.track_by_album.setdefault(track.album_path, set()).add(track.track_path)
        index.album_by_artist.setdefault(track.artist, set()).add(track.album_path)
        index.total_size += sum(track.file_sizes.values())

    # Populate dummy stats_by_location based on the tracks added
    index.stats_by_location["Main"] = {'file_count': 1, 'total_size': 1000, 'track_count': 1, 'album_count': 1, 'artist_count': 1}
    index.stats_by_location["Loc2"] = {'file_count': 1, 'total_size': 1500, 'track_count': 1, 'album_count': 1, 'artist_count': 1}
    index.stats_by_location["Loc3"] = {'file_count': 1, 'total_size': 2000, 'track_count': 1, 'album_count': 1, 'artist_count': 1}


    return index

# Replace original sample_index with the new symbolic one
@pytest.fixture
def sample_index(sample_index_symbolic):
    return sample_index_symbolic

def test_search_by_artist(sample_index):
    # Test case-sensitive search
    assert sample_index.search_by_artist("Artist1", case_sensitive=True) == ["Artist1"]
    assert sample_index.search_by_artist("artist1", case_sensitive=True) == []
    
    # Test case-insensitive search
    assert sorted(sample_index.search_by_artist("artist")) == ["Artist1", "Artist2"]
    assert sample_index.search_by_artist("nonexistent") == []
    
    # Test fuzzy search
    fuzzy_matches = sample_index.search_by_artist("Artst1", fuzzy_search=True)
    assert "Artist1" in fuzzy_matches  # The queried artist should be in the results
    assert len(fuzzy_matches) >= 1  # Should return at least one match
    assert all(isinstance(match, str) for match in fuzzy_matches)  # All matches should be strings

def test_search_by_album(sample_index):
    # Test without artist filter - results should include location prefix
    assert sorted(sample_index.search_by_album("Album")) == ["Loc2/Artist1/Album2", "Loc3/Artist2/Album3", "Main/Artist1/Album1"]
    assert sample_index.search_by_album("Album1") == ["Main/Artist1/Album1"] # Specific album name still works
    
    # Test with artist filter
    # Note: Artist filter doesn't use location prefix, but results do
    assert sorted(sample_index.search_by_album("Album", artist="Artist1")) == ["Loc2/Artist1/Album2", "Main/Artist1/Album1"]
    assert sample_index.search_by_album("Album3", artist="Artist2") == ["Loc3/Artist2/Album3"]
    assert sample_index.search_by_album("Album", artist="NonexistentArtist") == []

def test_search_by_track(sample_index):
    # Test basic track search
    tracks = sample_index.search_by_track("Track")
    assert len(tracks) == 3
    assert all(isinstance(t, TrackInfo) for t in tracks)
    # Verify tracks from different locations are found
    assert {t.track_path for t in tracks} == {
        "Main/Artist1/Album1/Track1",
        "Loc2/Artist1/Album2/Track2",
        "Loc3/Artist2/Album3/Track3"
    }
    
    # Test with artist filter
    tracks = sample_index.search_by_track("Track", artist="Artist1")
    assert len(tracks) == 2
    assert all(t.artist == "Artist1" for t in tracks)
    assert {t.track_path for t in tracks} == {"Main/Artist1/Album1/Track1", "Loc2/Artist1/Album2/Track2"}
    
    # Test with album filter (must use symbolic album path)
    tracks = sample_index.search_by_track("Track", album="Main/Artist1/Album1")
    assert len(tracks) == 1
    assert tracks[0].album_path == "Main/Artist1/Album1"
    assert tracks[0].track_path == "Main/Artist1/Album1/Track1"

    tracks_loc2 = sample_index.search_by_track("Track", album="Loc2/Artist1/Album2")
    assert len(tracks_loc2) == 1
    assert tracks_loc2[0].album_path == "Loc2/Artist1/Album2"
    assert tracks_loc2[0].track_path == "Loc2/Artist1/Album2/Track2"
    
    # Test with both filters
    tracks = sample_index.search_by_track("Track", artist="Artist1", album="Main/Artist1/Album1")
    assert len(tracks) == 1
    assert tracks[0].artist == "Artist1"
    assert tracks[0].album_path == "Main/Artist1/Album1"

def test_get_track_files(sample_index):
    # Test existing track (using symbolic path)
    files = sample_index.get_track_files("Main/Artist1/Album1/Track1")
    assert files == {"instrumental_audio": "Main/Artist1/Album1/Track1_instrumental.wav"}

    files_loc2 = sample_index.get_track_files("Loc2/Artist1/Album2/Track2")
    assert files_loc2 == {"instrumental_audio": "Loc2/Artist1/Album2/Track2_instrumental.wav"}
    
    # Test nonexistent track
    assert sample_index.get_track_files("nonexistent/track") == {}

# Add new test for index stats
def test_index_stats_by_location(sample_index):
    assert "stats_by_location" in sample_index.__dict__
    assert isinstance(sample_index.stats_by_location, dict)
    assert "Main" in sample_index.stats_by_location
    assert "Loc2" in sample_index.stats_by_location
    assert "Loc3" in sample_index.stats_by_location
    assert sample_index.stats_by_location["Main"]["total_size"] == 1000
    assert sample_index.stats_by_location["Loc2"]["total_size"] == 1500
    assert sample_index.stats_by_location["Loc3"]["total_size"] == 2000
    assert sample_index.total_size == 4500 # Verify aggregate total size 