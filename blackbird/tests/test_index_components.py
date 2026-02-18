"""Tests for index component analysis — verifying that DatasetIndex.build()
correctly counts and sizes components across the dataset."""

import pytest
import json
from pathlib import Path
from collections import defaultdict
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.schema import DatasetComponentSchema


@pytest.fixture
def multi_component_dataset(tmp_path):
    """Create a dataset with multiple component types and known sizes."""
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
    (blackbird_dir / "locations.json").write_text(
        json.dumps({"Main": str(dataset_root)})
    )

    # Artist1 / Album1 — 2 tracks, all 3 components each
    album1 = dataset_root / "Artist1" / "Album1 [2020]"
    album1.mkdir(parents=True)
    for i in range(1, 3):
        prefix = f"0{i}.Artist1 - Track{i}"
        (album1 / f"{prefix}_instrumental.mp3").write_bytes(b"\x00" * 4000)
        (album1 / f"{prefix}_vocals_noreverb.mp3").write_bytes(b"\x00" * 3000)
        (album1 / f"{prefix}.mir.json").write_text('{"bpm": 120}')

    # Artist2 / Album2 — 1 track, only instrumental + mir
    album2 = dataset_root / "Artist2" / "Album2 [2019]"
    album2.mkdir(parents=True)
    (album2 / "01.Artist2 - Song_instrumental.mp3").write_bytes(b"\x00" * 6000)
    (album2 / "01.Artist2 - Song.mir.json").write_text('{"bpm": 90}')

    return dataset_root


@pytest.fixture
def built_index(multi_component_dataset):
    """Build the index from the multi-component dataset."""
    schema = DatasetComponentSchema(multi_component_dataset)
    return DatasetIndex.build(multi_component_dataset, schema)


class TestComponentCounts:
    """Tests verifying correct component file counts after indexing."""

    def test_total_tracks(self, built_index):
        """Should have 3 tracks total (2 from Album1, 1 from Album2)."""
        assert len(built_index.tracks) == 3

    def test_all_tracks_have_instrumental(self, built_index):
        """Every track should have the instrumental component."""
        for track in built_index.tracks.values():
            assert "instrumental" in track.files

    def test_all_tracks_have_mir(self, built_index):
        """Every track should have the mir_analysis component."""
        for track in built_index.tracks.values():
            assert "mir_analysis" in track.files

    def test_vocals_count(self, built_index):
        """Only Album1 tracks (2) should have vocals."""
        count = sum(
            1 for t in built_index.tracks.values()
            if "vocals" in t.files
        )
        assert count == 2


class TestComponentSizes:
    """Tests verifying correct component file size tracking."""

    def test_total_size_positive(self, built_index):
        assert built_index.total_size > 0

    def test_instrumental_sizes(self, built_index):
        """Instrumental files should have the correct recorded sizes."""
        for track in built_index.tracks.values():
            if "instrumental" in track.files:
                path = track.files["instrumental"]
                size = track.file_sizes[path]
                # Our fixtures use 4000 or 6000 bytes for instrumentals
                assert size in (4000, 6000)

    def test_mir_json_sizes_small(self, built_index):
        """MIR JSON files should be much smaller than audio files."""
        for track in built_index.tracks.values():
            if "mir_analysis" in track.files:
                path = track.files["mir_analysis"]
                size = track.file_sizes[path]
                assert size < 100  # JSON content is tiny


class TestComponentAggregation:
    """Tests for aggregating component data across the index."""

    def test_component_count_by_type(self, built_index):
        """Manually count components and verify totals."""
        counts = defaultdict(int)
        for track in built_index.tracks.values():
            for comp_name in track.files:
                counts[comp_name] += 1

        assert counts["instrumental"] == 3
        assert counts["mir_analysis"] == 3
        assert counts["vocals"] == 2

    def test_component_total_size_by_type(self, built_index):
        """Sum sizes per component type."""
        sizes = defaultdict(int)
        for track in built_index.tracks.values():
            for comp_name, file_path in track.files.items():
                sizes[comp_name] += track.file_sizes[file_path]

        # 2 x 4000 + 1 x 6000 = 14000 for instrumental
        assert sizes["instrumental"] == 14000
        # 2 x 3000 = 6000 for vocals
        assert sizes["vocals"] == 6000

    def test_artist_album_structure(self, built_index):
        """Verify artist and album grouping is correct."""
        assert len(built_index.album_by_artist) == 2
        assert "Artist1" in built_index.album_by_artist
        assert "Artist2" in built_index.album_by_artist

    def test_stats_by_location(self, built_index):
        """Location stats should be populated for 'Main'."""
        assert "Main" in built_index.stats_by_location
        main_stats = built_index.stats_by_location["Main"]
        assert main_stats["track_count"] == 3
        assert main_stats["artist_count"] == 2
        assert main_stats["total_size"] > 0
