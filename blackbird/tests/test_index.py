import pytest
from pathlib import Path
from datetime import datetime
from blackbird.index import DatasetIndex, TrackInfo

@pytest.fixture
def sample_index():
    index = DatasetIndex.create()
    
    # Add some sample data
    track1 = TrackInfo(
        track_path="Artist1/Album1/Track1",
        artist="Artist1",
        album_path="Artist1/Album1",
        cd_number=None,
        base_name="Track1",
        files={"instrumental_audio": "Artist1/Album1/Track1_instrumental.wav"},
        file_sizes={"Artist1/Album1/Track1_instrumental.wav": 1000}
    )
    
    track2 = TrackInfo(
        track_path="Artist1/Album2/Track2",
        artist="Artist1",
        album_path="Artist1/Album2",
        cd_number=None,
        base_name="Track2",
        files={"instrumental_audio": "Artist1/Album2/Track2_instrumental.wav"},
        file_sizes={"Artist1/Album2/Track2_instrumental.wav": 1000}
    )
    
    track3 = TrackInfo(
        track_path="Artist2/Album3/Track3",
        artist="Artist2",
        album_path="Artist2/Album3",
        cd_number=None,
        base_name="Track3",
        files={"instrumental_audio": "Artist2/Album3/Track3_instrumental.wav"},
        file_sizes={"Artist2/Album3/Track3_instrumental.wav": 1000}
    )
    
    # Add tracks to index
    for track in [track1, track2, track3]:
        index.tracks[track.track_path] = track
        index.track_by_album.setdefault(track.album_path, set()).add(track.track_path)
        index.album_by_artist.setdefault(track.artist, set()).add(track.album_path)
        index.total_size += sum(track.file_sizes.values())
    
    return index

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
    # Test without artist filter
    assert sorted(sample_index.search_by_album("Album")) == ["Artist1/Album1", "Artist1/Album2", "Artist2/Album3"]
    assert sample_index.search_by_album("Album1") == ["Artist1/Album1"]
    
    # Test with artist filter
    assert sample_index.search_by_album("Album", artist="Artist1") == ["Artist1/Album1", "Artist1/Album2"]
    assert sample_index.search_by_album("Album3", artist="Artist2") == ["Artist2/Album3"]
    assert sample_index.search_by_album("Album", artist="NonexistentArtist") == []

def test_search_by_track(sample_index):
    # Test basic track search
    tracks = sample_index.search_by_track("Track")
    assert len(tracks) == 3
    assert all(isinstance(t, TrackInfo) for t in tracks)
    
    # Test with artist filter
    tracks = sample_index.search_by_track("Track", artist="Artist1")
    assert len(tracks) == 2
    assert all(t.artist == "Artist1" for t in tracks)
    
    # Test with album filter
    tracks = sample_index.search_by_track("Track", album="Artist1/Album1")
    assert len(tracks) == 1
    assert tracks[0].album_path == "Artist1/Album1"
    
    # Test with both filters
    tracks = sample_index.search_by_track("Track", artist="Artist1", album="Artist1/Album1")
    assert len(tracks) == 1
    assert tracks[0].artist == "Artist1"
    assert tracks[0].album_path == "Artist1/Album1"

def test_get_track_files(sample_index):
    # Test existing track
    files = sample_index.get_track_files("Artist1/Album1/Track1")
    assert files == {"instrumental_audio": "Artist1/Album1/Track1_instrumental.wav"}
    
    # Test nonexistent track
    assert sample_index.get_track_files("nonexistent/track") == {} 