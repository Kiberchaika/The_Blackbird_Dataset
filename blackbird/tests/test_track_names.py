"""Tests for schema track name resolution, companion files, and CD structure handling."""

import pytest
import json
from pathlib import Path
from blackbird.schema import DatasetComponentSchema


@pytest.fixture
def dataset_with_tracks(tmp_path):
    """Create a dataset with various track naming patterns."""
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)

    schema_data = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "multiple": False,
                "description": "Instrumental"
            },
            "vocals": {
                "pattern": "*_vocals_noreverb.mp3",
                "multiple": False,
                "description": "Vocals"
            },
            "mir_analysis": {
                "pattern": "*.mir.json",
                "multiple": False,
                "description": "MIR analysis"
            }
        }
    }
    (blackbird_dir / "schema.json").write_text(json.dumps(schema_data))

    # Regular album
    album1 = dataset_root / "Artist1" / "Album1 [2020]"
    album1.mkdir(parents=True)
    for name in [
        "01.Artist1 - Track One_instrumental.mp3",
        "01.Artist1 - Track One_vocals_noreverb.mp3",
        "01.Artist1 - Track One.mir.json",
        "02.Artist1 - Track Two_instrumental.mp3",
    ]:
        (album1 / name).write_bytes(b"\x00" * 100)

    # Second artist
    album2 = dataset_root / "Artist2" / "Album2 [2019]"
    album2.mkdir(parents=True)
    for name in [
        "01.Artist2 - Song_instrumental.mp3",
        "01.Artist2 - Song.mir.json",
    ]:
        (album2 / name).write_bytes(b"\x00" * 100)

    return dataset_root


@pytest.fixture
def cd_dataset(tmp_path):
    """Create a dataset with multi-CD album structure."""
    dataset_root = tmp_path / "dataset"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)

    schema_data = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "multiple": False,
                "description": "Instrumental"
            }
        }
    }
    (blackbird_dir / "schema.json").write_text(json.dumps(schema_data))

    # Multi-CD album
    album = dataset_root / "Artist1" / "DoubleAlbum [2020]"
    cd1 = album / "CD1"
    cd2 = album / "CD2"
    cd1.mkdir(parents=True)
    cd2.mkdir(parents=True)

    (cd1 / "01.Track A_instrumental.mp3").write_bytes(b"\x00" * 100)
    (cd2 / "01.Track B_instrumental.mp3").write_bytes(b"\x00" * 100)

    # Regular album for comparison
    regular = dataset_root / "Artist1" / "SingleAlbum [2021]"
    regular.mkdir(parents=True)
    (regular / "01.Track C_instrumental.mp3").write_bytes(b"\x00" * 100)

    return dataset_root


class TestGetTrackRelativePath:
    """Tests for DatasetComponentSchema.get_track_relative_path()."""

    def test_strips_component_suffix(self, dataset_with_tracks):
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "01.Artist1 - Track One_instrumental.mp3"
        rel = schema.get_track_relative_path(track_file)
        assert "_instrumental" not in rel
        assert "01.Artist1 - Track One" in rel

    def test_strips_vocals_suffix(self, dataset_with_tracks):
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "01.Artist1 - Track One_vocals_noreverb.mp3"
        rel = schema.get_track_relative_path(track_file)
        assert "_vocals_noreverb" not in rel

    def test_preserves_artist_album_structure(self, dataset_with_tracks):
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "01.Artist1 - Track One_instrumental.mp3"
        rel = schema.get_track_relative_path(track_file)
        assert "Artist1" in rel
        assert "Album1 [2020]" in rel

    def test_same_base_for_different_components(self, dataset_with_tracks):
        """Different component files for the same track should resolve to the same base path."""
        schema = DatasetComponentSchema(dataset_with_tracks)
        album = dataset_with_tracks / "Artist1" / "Album1 [2020]"
        instrumental = schema.get_track_relative_path(album / "01.Artist1 - Track One_instrumental.mp3")
        vocals = schema.get_track_relative_path(album / "01.Artist1 - Track One_vocals_noreverb.mp3")
        # Both should resolve to the same track base
        assert instrumental == vocals

    def test_cd_track_path(self, cd_dataset):
        """Tracks in CD directories include the CD in the relative path."""
        schema = DatasetComponentSchema(cd_dataset)
        track_file = cd_dataset / "Artist1" / "DoubleAlbum [2020]" / "CD1" / "01.Track A_instrumental.mp3"
        rel = schema.get_track_relative_path(track_file)
        assert "CD1" in rel


class TestFindCompanionFiles:
    """Tests for DatasetComponentSchema.find_companion_files()."""

    def test_finds_companion_files(self, dataset_with_tracks):
        """Given an instrumental file, find the other component files."""
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "01.Artist1 - Track One_instrumental.mp3"
        companions = schema.find_companion_files(track_file)
        companion_names = {c.name for c in companions}
        # Should find vocals and/or mir.json as companions
        assert len(companions) > 0
        assert any("vocals_noreverb" in n or "mir.json" in n for n in companion_names)

    def test_does_not_include_self(self, dataset_with_tracks):
        """The source file should not be in the companion list."""
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "01.Artist1 - Track One_instrumental.mp3"
        companions = schema.find_companion_files(track_file)
        assert track_file not in companions

    def test_track_with_fewer_components(self, dataset_with_tracks):
        """Track Two only has instrumental (no vocals/mir), so no companions."""
        schema = DatasetComponentSchema(dataset_with_tracks)
        track_file = dataset_with_tracks / "Artist1" / "Album1 [2020]" / "02.Artist1 - Track Two_instrumental.mp3"
        companions = schema.find_companion_files(track_file)
        # Track Two only has _instrumental.mp3, no other components
        assert len(companions) == 0


class TestCDStructure:
    """Tests for multi-CD album handling."""

    def test_cd_dirs_are_valid_structure(self, cd_dataset):
        """Schema validation should pass for albums with CD subdirectories."""
        schema = DatasetComponentSchema(cd_dataset)
        result = schema.validate()
        assert result.is_valid

    def test_tracks_in_different_cds_are_distinct(self, cd_dataset):
        """Tracks in CD1 and CD2 should have different relative paths."""
        schema = DatasetComponentSchema(cd_dataset)
        track_a = cd_dataset / "Artist1" / "DoubleAlbum [2020]" / "CD1" / "01.Track A_instrumental.mp3"
        track_b = cd_dataset / "Artist1" / "DoubleAlbum [2020]" / "CD2" / "01.Track B_instrumental.mp3"
        path_a = schema.get_track_relative_path(track_a)
        path_b = schema.get_track_relative_path(track_b)
        assert path_a != path_b

    def test_cd_and_regular_albums_coexist(self, cd_dataset):
        """Both CD and regular albums should be handled."""
        schema = DatasetComponentSchema(cd_dataset)
        # CD track
        cd_track = cd_dataset / "Artist1" / "DoubleAlbum [2020]" / "CD1" / "01.Track A_instrumental.mp3"
        # Regular track
        reg_track = cd_dataset / "Artist1" / "SingleAlbum [2021]" / "01.Track C_instrumental.mp3"
        cd_rel = schema.get_track_relative_path(cd_track)
        reg_rel = schema.get_track_relative_path(reg_track)
        assert "DoubleAlbum" in cd_rel
        assert "SingleAlbum" in reg_rel
