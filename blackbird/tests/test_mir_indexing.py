"""Tests for .mir.json file indexing via DatasetIndex.build()."""

import pytest
import json
from pathlib import Path
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.schema import DatasetComponentSchema


@pytest.fixture
def mir_dataset(tmp_path):
    """Create a temporary dataset with .mir.json files and matching originals."""
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)

    # Schema that includes both original audio and mir.json components
    schema_data = {
        "version": "1.0",
        "components": {
            "original_audio": {
                "pattern": "*.mp3",
                "multiple": False,
                "description": "Original audio"
            },
            "mir_analysis": {
                "pattern": "*.mir.json",
                "multiple": False,
                "description": "MIR analysis results"
            }
        }
    }
    (blackbird_dir / "schema.json").write_text(json.dumps(schema_data))

    # Locations config pointing at dataset root
    locations_data = {"Main": str(dataset_root)}
    (blackbird_dir / "locations.json").write_text(json.dumps(locations_data))

    # Artist1 / Album1 - two tracks with both components
    album1 = dataset_root / "Artist1" / "Album1 [2020]"
    album1.mkdir(parents=True)
    (album1 / "01.Artist1 - Track One.mp3").write_bytes(b"\x00" * 1000)
    (album1 / "01.Artist1 - Track One.mir.json").write_text('{"bpm": 120}')
    (album1 / "02.Artist1 - Track Two.mp3").write_bytes(b"\x00" * 2000)
    (album1 / "02.Artist1 - Track Two.mir.json").write_text('{"bpm": 90}')

    # Artist1 / Album2 - one track, mir.json only (no mp3)
    album2 = dataset_root / "Artist1" / "Album2 [2021]"
    album2.mkdir(parents=True)
    (album2 / "01.Artist1 - Solo.mir.json").write_text('{"bpm": 140}')

    # Artist2 / Album3 - one track with both components
    album3 = dataset_root / "Artist2" / "Album3 [2019]"
    album3.mkdir(parents=True)
    (album3 / "01.Artist2 - Song.mp3").write_bytes(b"\x00" * 3000)
    (album3 / "01.Artist2 - Song.mir.json").write_text('{"bpm": 110}')

    return dataset_root


@pytest.fixture
def mir_dataset_with_cds(tmp_path):
    """Create a dataset with CD subdirectories containing .mir.json files."""
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)

    schema_data = {
        "version": "1.0",
        "components": {
            "original_audio": {
                "pattern": "*.mp3",
                "multiple": False,
                "description": "Original audio"
            },
            "mir_analysis": {
                "pattern": "*.mir.json",
                "multiple": False,
                "description": "MIR analysis results"
            }
        }
    }
    (blackbird_dir / "schema.json").write_text(json.dumps(schema_data))
    (blackbird_dir / "locations.json").write_text(
        json.dumps({"Main": str(dataset_root)})
    )

    # Album with two CDs
    cd1 = dataset_root / "Artist1" / "DoubleAlbum [2020]" / "CD1"
    cd2 = dataset_root / "Artist1" / "DoubleAlbum [2020]" / "CD2"
    cd1.mkdir(parents=True)
    cd2.mkdir(parents=True)

    (cd1 / "01.Artist1 - Intro.mp3").write_bytes(b"\x00" * 500)
    (cd1 / "01.Artist1 - Intro.mir.json").write_text('{"bpm": 100}')
    (cd2 / "01.Artist1 - Outro.mp3").write_bytes(b"\x00" * 600)
    (cd2 / "01.Artist1 - Outro.mir.json").write_text('{"bpm": 80}')

    return dataset_root


@pytest.fixture
def build_index(mir_dataset):
    """Build a DatasetIndex from the mir_dataset fixture."""
    schema = DatasetComponentSchema(mir_dataset)
    return DatasetIndex.build(mir_dataset, schema)


class TestMirIndexing:
    """Tests for indexing datasets that contain .mir.json files."""

    def test_mir_files_are_indexed(self, build_index):
        """mir.json files should appear as a component on indexed tracks."""
        index = build_index
        mir_tracks = [
            t for t in index.tracks.values()
            if "mir_analysis" in t.files
        ]
        # All 4 tracks that have .mir.json files should be indexed
        assert len(mir_tracks) == 4

    def test_track_has_mir_component(self, build_index):
        """Each track with a .mir.json file should have a mir_analysis component."""
        index = build_index
        for track in index.tracks.values():
            if "mir_analysis" in track.files:
                mir_path = track.files["mir_analysis"]
                assert mir_path.endswith(".mir.json")

    def test_mir_file_sizes_are_recorded(self, build_index):
        """File sizes for .mir.json files should be non-zero."""
        index = build_index
        for track in index.tracks.values():
            if "mir_analysis" in track.files:
                mir_path = track.files["mir_analysis"]
                assert mir_path in track.file_sizes
                assert track.file_sizes[mir_path] > 0

    def test_artists_are_indexed(self, build_index):
        """Both artists should appear in the index."""
        index = build_index
        assert len(index.album_by_artist) == 2
        artists = set(index.album_by_artist.keys())
        assert "Artist1" in artists
        assert "Artist2" in artists

    def test_albums_per_artist(self, build_index):
        """Artist1 should have 2 albums, Artist2 should have 1."""
        index = build_index
        artist1_albums = index.album_by_artist["Artist1"]
        artist2_albums = index.album_by_artist["Artist2"]
        assert len(artist1_albums) == 2
        assert len(artist2_albums) == 1

    def test_tracks_per_album(self, build_index):
        """Album1 should have 2 tracks, Album2 and Album3 should have 1 each."""
        index = build_index
        # Find album paths by album name substring
        album_track_counts = {}
        for album_path, track_paths in index.track_by_album.items():
            # Extract album name from symbolic path (Main/Artist/Album)
            album_name = album_path.split("/")[-1]
            album_track_counts[album_name] = len(track_paths)

        assert album_track_counts["Album1 [2020]"] == 2
        assert album_track_counts["Album2 [2021]"] == 1
        assert album_track_counts["Album3 [2019]"] == 1

    def test_total_size_is_positive(self, build_index):
        """Total index size should reflect all indexed files."""
        assert build_index.total_size > 0

    def test_track_without_mp3_still_indexed(self, build_index):
        """A track that only has .mir.json (no .mp3) should still be indexed."""
        index = build_index
        # Find the track in Album2 (which only has a .mir.json, no .mp3)
        solo_tracks = [
            t for t in index.tracks.values()
            if "Album2" in t.album_path
        ]
        assert len(solo_tracks) == 1
        assert "mir_analysis" in solo_tracks[0].files
        assert "original_audio" not in solo_tracks[0].files

    def test_track_with_both_components(self, build_index):
        """Tracks that have both .mp3 and .mir.json should have both components."""
        index = build_index
        dual_tracks = [
            t for t in index.tracks.values()
            if "mir_analysis" in t.files and "original_audio" in t.files
        ]
        # 3 tracks have both: Album1 Track One, Album1 Track Two, Album3 Song
        assert len(dual_tracks) == 3


class TestMirIndexingWithCDs:
    """Tests for .mir.json indexing in albums with CD subdirectories."""

    def test_cd_tracks_are_indexed(self, mir_dataset_with_cds):
        schema = DatasetComponentSchema(mir_dataset_with_cds)
        index = DatasetIndex.build(mir_dataset_with_cds, schema)

        assert len(index.tracks) == 2
        cd_numbers = {t.cd_number for t in index.tracks.values()}
        assert cd_numbers == {"CD1", "CD2"}

    def test_cd_tracks_share_album(self, mir_dataset_with_cds):
        schema = DatasetComponentSchema(mir_dataset_with_cds)
        index = DatasetIndex.build(mir_dataset_with_cds, schema)

        album_paths = {t.album_path for t in index.tracks.values()}
        assert len(album_paths) == 1
        assert "DoubleAlbum [2020]" in next(iter(album_paths))

    def test_cd_tracks_have_mir_component(self, mir_dataset_with_cds):
        schema = DatasetComponentSchema(mir_dataset_with_cds)
        index = DatasetIndex.build(mir_dataset_with_cds, schema)

        for track in index.tracks.values():
            assert "mir_analysis" in track.files
            assert track.files["mir_analysis"].endswith(".mir.json")


class TestMirIndexingSaveLoad:
    """Tests for saving and loading an index with .mir.json data."""

    def test_round_trip(self, build_index, tmp_path):
        """Index with mir data should survive save/load."""
        index_path = tmp_path / "index.pickle"
        build_index.save(index_path)

        loaded = DatasetIndex.load(index_path)

        assert len(loaded.tracks) == len(build_index.tracks)
        assert loaded.total_size == build_index.total_size
        assert set(loaded.album_by_artist.keys()) == set(build_index.album_by_artist.keys())

        # Verify mir components survived
        for track_path, track in loaded.tracks.items():
            original = build_index.tracks[track_path]
            assert track.files == original.files
            assert track.file_sizes == original.file_sizes
