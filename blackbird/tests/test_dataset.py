import pytest
from pathlib import Path
import json
import shutil
from blackbird.dataset import Dataset
from blackbird.schema import DatasetComponentSchema

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
    assert len(dataset.index.tracks) == 4  # Total number of tracks
    assert len(dataset.index.album_by_artist) == 2  # Two artists
    assert dataset.index.total_size >= 0  # Size should be calculated

def test_find_tracks_all(test_dataset):
    """Test finding all tracks."""
    dataset = Dataset(test_dataset)
    tracks = dataset.find_tracks()
    
    # Should find all instrumental tracks (including CD tracks)
    assert len(tracks) == 4
    
    # Verify track paths
    track_paths = set(tracks.keys())
    expected_paths = {
        "Artist1/Album1/track1",
        "Artist1/Album1/track2",
        "Artist2/Album1/CD1/track1",
        "Artist2/Album1/CD2/track1"
    }
    assert track_paths == expected_paths

def test_find_tracks_with_components(test_dataset):
    """Test finding tracks with specific components."""
    dataset = Dataset(test_dataset)
    
    # Find tracks with vocals
    tracks_with_vocals = dataset.find_tracks(has=["vocals"])
    assert len(tracks_with_vocals) == 2
    
    # Find tracks with both vocals and mir
    complete_tracks = dataset.find_tracks(has=["vocals", "mir"])
    assert len(complete_tracks) == 1
    track_path = next(iter(complete_tracks.keys()))
    assert track_path == "Artist1/Album1/track1"

def test_find_tracks_missing_components(test_dataset):
    """Test finding tracks missing components."""
    dataset = Dataset(test_dataset)
    
    # Find tracks missing vocals
    tracks_no_vocals = dataset.find_tracks(missing=["vocals"])
    assert len(tracks_no_vocals) == 2  # track2 and CD2/track1
    
    # Verify specific tracks
    track_paths = set(tracks_no_vocals.keys())
    expected_paths = {
        "Artist1/Album1/track2",
        "Artist2/Album1/CD2/track1"
    }
    assert track_paths == expected_paths

def test_find_tracks_by_artist(test_dataset):
    """Test finding tracks filtered by artist."""
    dataset = Dataset(test_dataset)
    
    # Find Artist1's tracks
    artist1_tracks = dataset.find_tracks(artist="Artist1")
    assert len(artist1_tracks) == 2
    
    # Find Artist2's tracks (both CD1 and CD2)
    artist2_tracks = dataset.find_tracks(artist="Artist2")
    assert len(artist2_tracks) == 2
    
    # Verify CD tracks
    track_paths = set(artist2_tracks.keys())
    expected_paths = {
        "Artist2/Album1/CD1/track1",
        "Artist2/Album1/CD2/track1"
    }
    assert track_paths == expected_paths

def test_analyze_dataset(test_dataset):
    """Test dataset analysis."""
    dataset = Dataset(test_dataset)
    stats = dataset.analyze()
    
    assert stats["tracks"]["total"] == 4  # Including both CD tracks
    assert len(stats["artists"]) == 2
    assert stats["components"]["instrumental"] == 4  # All tracks have instrumental
    assert stats["components"]["vocals"] == 2  # Two tracks have vocals
    assert stats["components"]["mir"] == 2  # Two tracks have MIR
    
    # Check artist-specific stats
    assert stats["tracks"]["by_artist"]["Artist1"] == 2
    assert stats["tracks"]["by_artist"]["Artist2"] == 2  # Both CD tracks
    
    # Check album structure
    assert "Album1" in stats["albums"]["Artist1"]
    assert "Album1" in stats["albums"]["Artist2"]

def test_rebuild_index(test_dataset):
    """Test manual index rebuilding."""
    dataset = Dataset(test_dataset)
    initial_index = dataset.index
    
    # Add a new file
    (test_dataset / "Artist1" / "Album1" / "track3_instrumental.mp3").touch()
    
    # Rebuild index
    dataset.rebuild_index()
    
    # Verify index was updated
    assert len(dataset.index.tracks) == len(initial_index.tracks) + 1
    assert "Artist1/Album1/track3" in dataset.index.tracks 