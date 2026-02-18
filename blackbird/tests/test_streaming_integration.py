"""Integration tests for StreamingPipeline with a dummy dataset.

dataset1 is a single temporary dataset — the pipeline downloads source files
from it, processes them, and uploads results back to the same dataset.

Structure: 10 artists × 2 albums × 5 tracks × 2 components = 200 files
"""

import json
import os
import pickle
import shutil
import random
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from blackbird.index import DatasetIndex, TrackInfo
from blackbird.streaming import StreamingPipeline, _PipelineState


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARTISTS = [f"Artist_{i:02d}" for i in range(10)]
ALBUMS_PER_ARTIST = 2
TRACKS_PER_ALBUM = 5
COMPONENTS = {
    "original": "*_original.mp3",
    "vocal": "*_vocal.mp3",
}
# 10 artists * 2 albums * 5 tracks = 100 tracks
# 100 tracks * 2 components = 200 files total

AUDIO_CONTENT_SIZE = 4096  # bytes per fake audio file


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dataset1(tmp_path):
    """Create a dummy dataset with 200 audio files on disk.

    Structure:
        dataset1/
        ├── .blackbird/
        │   ├── schema.json
        │   └── index.pickle
        ├── Artist_00/
        │   ├── Album_0/
        │   │   ├── track_0_original.mp3
        │   │   ├── track_0_vocal.mp3
        │   │   └── ...
        │   └── Album_1/
        │       └── ...
        └── ...
    """
    root = tmp_path / "dataset1"
    root.mkdir()

    rng = random.Random(42)

    schema_data = {
        "version": "1.0",
        "components": {
            name: {"pattern": pattern, "multiple": False, "description": ""}
            for name, pattern in COMPONENTS.items()
        },
    }

    blackbird_dir = root / ".blackbird"
    blackbird_dir.mkdir()
    with open(blackbird_dir / "schema.json", "w") as f:
        json.dump(schema_data, f, indent=2)

    index = DatasetIndex.create()
    location_name = "Main"

    for artist in ARTISTS:
        for album_idx in range(ALBUMS_PER_ARTIST):
            album = f"Album_{album_idx}"
            album_dir = root / artist / album
            album_dir.mkdir(parents=True)

            for track_idx in range(TRACKS_PER_ALBUM):
                base_name = f"track_{track_idx}"
                symbolic_album_path = f"{location_name}/{artist}/{album}"
                symbolic_track_path = f"{symbolic_album_path}/{base_name}"

                files = {}
                file_sizes = {}

                for comp_name in COMPONENTS:
                    filename = f"{base_name}_{comp_name}.mp3"
                    file_path = album_dir / filename
                    content = rng.randbytes(AUDIO_CONTENT_SIZE)
                    file_path.write_bytes(content)

                    symbolic_file = f"{location_name}/{artist}/{album}/{filename}"
                    files[comp_name] = symbolic_file
                    file_sizes[symbolic_file] = AUDIO_CONTENT_SIZE

                    file_hash = hash(symbolic_file)
                    index.file_info_by_hash[file_hash] = (symbolic_file, AUDIO_CONTENT_SIZE)
                    index.total_size += AUDIO_CONTENT_SIZE

                track = TrackInfo(
                    track_path=symbolic_track_path,
                    artist=artist,
                    album_path=symbolic_album_path,
                    cd_number=None,
                    base_name=base_name,
                    files=files,
                    file_sizes=file_sizes,
                )
                index.tracks[symbolic_track_path] = track
                index.track_by_album.setdefault(symbolic_album_path, set()).add(
                    symbolic_track_path
                )
                index.album_by_artist.setdefault(artist, set()).add(symbolic_album_path)

    index.last_updated = datetime.now()
    index.save(blackbird_dir / "index.pickle")

    return root


@pytest.fixture
def work_dir(tmp_path):
    return tmp_path / "pipeline_work"


def _make_mock(dataset_root: Path):
    """Mock WebDAVClient: download AND upload both target the same dataset_root.

    download_file: reads from dataset_root/
    upload_file:   writes into dataset_root/  (same server)
    """
    mock = MagicMock()

    def download_side_effect(remote_path, local_path, **kwargs):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        source = dataset_root / remote_path
        if source.exists():
            shutil.copy2(source, local_path)
            return True
        return False

    def upload_side_effect(local_path, remote_path):
        local_path = Path(local_path)
        if not local_path.exists():
            return False
        dest = dataset_root / remote_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)
        return True

    mock.download_file = MagicMock(side_effect=download_side_effect)
    mock.upload_file = MagicMock(side_effect=upload_side_effect)

    return mock


# ---------------------------------------------------------------------------
# Tests: dataset structure
# ---------------------------------------------------------------------------

class TestDatasetStructure:
    """Verify the dummy dataset is created correctly."""

    def test_has_100_original_files(self, dataset1):
        assert len(list(dataset1.rglob("*_original.mp3"))) == 100

    def test_has_100_vocal_files(self, dataset1):
        assert len(list(dataset1.rglob("*_vocal.mp3"))) == 100

    def test_has_200_total_audio_files(self, dataset1):
        assert len(list(dataset1.rglob("*.mp3"))) == 200

    def test_has_valid_schema(self, dataset1):
        with open(dataset1 / ".blackbird" / "schema.json") as f:
            schema = json.load(f)
        assert "original" in schema["components"]
        assert "vocal" in schema["components"]

    def test_has_valid_index(self, dataset1):
        index = DatasetIndex.load(dataset1 / ".blackbird" / "index.pickle")
        assert len(index.tracks) == 100
        assert len(index.album_by_artist) == 10

    def test_files_have_content(self, dataset1):
        some_file = next(dataset1.rglob("*_original.mp3"))
        assert some_file.stat().st_size == AUDIO_CONTENT_SIZE

    def test_no_mir_json_initially(self, dataset1):
        assert list(dataset1.rglob("*.mir.json")) == []


# ---------------------------------------------------------------------------
# Tests: full pipeline (download → process → upload back to same dataset)
# ---------------------------------------------------------------------------

class TestFullPipeline:
    """Download from dataset1, process, upload results back to dataset1."""

    def test_process_all_originals(self, dataset1, work_dir):
        """Process 100 originals, upload .mir.json results back to dataset1."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=10,
                prefetch_workers=4,
                upload_workers=2,
                work_dir=str(work_dir),
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=4)
                    if not items:
                        break
                    for item in items:
                        assert item.local_path.exists()
                        assert item.local_path.stat().st_size == AUDIO_CONTENT_SIZE

                        result_path = item.local_path.with_suffix(".mir.json")
                        result_path.write_text(json.dumps({
                            "bpm": 120, "key": "C", "source": item.remote_path,
                        }))

                        pipeline.submit_result(
                            item=item,
                            result_path=result_path,
                            remote_name=f"{item.metadata['track']}.mir.json",
                        )
                        processed += 1

        # All 100 originals processed
        assert processed == 100

        # 100 .mir.json appeared in dataset1 (uploaded back)
        results = list(dataset1.rglob("*.mir.json"))
        assert len(results) == 100

        # Results sit next to source files
        sample = results[0]
        assert sample.parent.exists()
        # Same directory should also contain original mp3
        mp3s_in_dir = list(sample.parent.glob("*_original.mp3"))
        assert len(mp3s_in_dir) > 0

        # Local work_dir cleaned up
        downloads = work_dir / "downloads"
        leftover = list(downloads.rglob("*.*")) if downloads.exists() else []
        assert len(leftover) == 0

        # State file removed on success
        assert not (work_dir / ".pipeline_state.json").exists()

    def test_process_only_vocals(self, dataset1, work_dir):
        """Filter by component=vocal."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["vocal"],
                queue_size=8,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        assert item.metadata["component"] == "vocal"
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("processed")
                        pipeline.submit_result(item, result_path, "result.out")
                        processed += 1

        assert processed == 100

    def test_filter_by_artist(self, dataset1, work_dir):
        """Process only Artist_00."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                queue_size=8,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        assert item.metadata["artist"] == "Artist_00"
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("done")
                        pipeline.submit_result(item, result_path, "r.out")
                        processed += 1

        assert processed == 10  # 2 albums * 5 tracks

    def test_filter_by_album(self, dataset1, work_dir):
        """Process only Album_0 across all artists."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                albums=["Album_0"],
                queue_size=8,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        assert item.metadata["album"] == "Album_0"
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("done")
                        pipeline.submit_result(item, result_path, "r.out")
                        processed += 1

        assert processed == 50  # 10 artists * 5 tracks

    def test_skip_all_files(self, dataset1, work_dir):
        """Skip everything — nothing uploaded."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                queue_size=8,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            skipped = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        pipeline.skip(item)
                        skipped += 1

        assert skipped == 10
        assert mock.upload_file.call_count == 0
        # No new files in dataset1
        assert list(dataset1.rglob("*.mir.json")) == []


# ---------------------------------------------------------------------------
# Tests: resume
# ---------------------------------------------------------------------------

class TestResume:
    """Resume after interruption."""

    def test_resume_skips_already_processed(self, dataset1, work_dir):
        """Pre-mark 50 files as processed, only remaining 50 run."""
        mock = _make_mock(dataset1)

        work_dir.mkdir(parents=True, exist_ok=True)
        state = _PipelineState(url="webdav://host/data")
        index = DatasetIndex.load(dataset1 / ".blackbird" / "index.pickle")

        count = 0
        for track_path, track_info in index.tracks.items():
            if count >= 50:
                break
            for comp_name, symbolic_file_path in track_info.files.items():
                if comp_name == "original":
                    parts = symbolic_file_path.split('/', 1)
                    remote_path = parts[1] if len(parts) == 2 else symbolic_file_path
                    state.processed.append(remote_path)
                    count += 1
                    if count >= 50:
                        break

        state.save(work_dir / ".pipeline_state.json")

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=8,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("done")
                        pipeline.submit_result(item, result_path, "r.out")
                        processed += 1

        assert processed == 50

    def test_two_pass_processing(self, dataset1, work_dir):
        """Run pipeline twice — second run finds nothing to do."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                queue_size=8,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            first_pass = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("done")
                        pipeline.submit_result(item, result_path, "r.out")
                        first_pass += 1

        assert first_pass == 10

        # Recreate state (was cleaned on success — simulate crash scenario)
        state = _PipelineState(url="webdav://host/data")
        index = DatasetIndex.load(dataset1 / ".blackbird" / "index.pickle")
        for track_path, track_info in index.tracks.items():
            if track_info.artist == "Artist_00":
                for comp_name, sym in track_info.files.items():
                    if comp_name == "original":
                        parts = sym.split('/', 1)
                        state.processed.append(parts[1] if len(parts) == 2 else sym)
        work_dir.mkdir(parents=True, exist_ok=True)
        state.save(work_dir / ".pipeline_state.json")

        mock2 = _make_mock(dataset1)
        with patch("blackbird.streaming.configure_client", return_value=mock2):
            pipeline2 = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                queue_size=8,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            second_pass = 0
            with pipeline2:
                while True:
                    items = pipeline2.take(count=1)
                    if not items:
                        break
                    second_pass += 1

        assert second_pass == 0


# ---------------------------------------------------------------------------
# Tests: batch sizes
# ---------------------------------------------------------------------------

class TestBatchProcessing:

    def test_take_count_1(self, dataset1, work_dir):
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                albums=["Album_0"],
                queue_size=4,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            batch_sizes = []
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    batch_sizes.append(len(items))
                    for item in items:
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("x")
                        pipeline.submit_result(item, result_path, "r.out")

        assert all(s == 1 for s in batch_sizes)
        assert sum(batch_sizes) == 5

    def test_take_count_larger_than_remaining(self, dataset1, work_dir):
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_00"],
                albums=["Album_0"],
                queue_size=10,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            with pipeline:
                items = pipeline.take(count=100)
                assert len(items) == 5
                for item in items:
                    pipeline.skip(item)


# ---------------------------------------------------------------------------
# Tests: spec usage pattern
# ---------------------------------------------------------------------------

class TestSpecExample:
    """Test the exact usage pattern from the spec."""

    def test_spec_usage_pattern(self, dataset1, work_dir):
        """Exact code from streaming_pipeline_spec.md — process and upload back."""
        mock = _make_mock(dataset1)

        def run_mir_analysis(audio_path: Path) -> dict:
            data = audio_path.read_bytes()
            return {
                "bpm": 120 + (len(data) % 60),
                "key": "Cm",
                "duration_sec": len(data) / 44100,
                "rms_energy": sum(data[:100]) / 100,
                "source_file": audio_path.name,
            }

        def save_json(obj, path):
            Path(path).write_text(json.dumps(obj, indent=2))

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="https://my-server.com/dataset",
                components=["original"],
                queue_size=8,
                prefetch_workers=4,
                upload_workers=2,
                work_dir=str(work_dir),
                username="user",
                password="pass",
            )

            processed = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=4)
                    if not items:
                        break

                    for item in items:
                        result = run_mir_analysis(item.local_path)
                        save_path = item.local_path.with_suffix(".mir.json")
                        save_json(result, save_path)

                        pipeline.submit_result(
                            item=item,
                            result_path=save_path,
                            remote_name=item.metadata["track"] + ".mir.json",
                        )
                        processed += 1

        assert processed == 100

        # Results uploaded back to dataset1
        results = list(dataset1.rglob("*.mir.json"))
        assert len(results) == 100

        # Each result is valid JSON with expected fields
        for r in results:
            data = json.loads(r.read_text())
            assert "bpm" in data
            assert "key" in data
            assert "source_file" in data

        # Results sit next to original mp3s in the same dirs
        for r in results:
            rel = r.relative_to(dataset1)
            parts = rel.parts
            assert len(parts) == 3  # Artist/Album/file.mir.json
            assert parts[0].startswith("Artist_")
            assert parts[1].startswith("Album_")
            assert parts[2].endswith(".mir.json")
            # Same dir has original mp3
            assert len(list(r.parent.glob("*_original.mp3"))) > 0

        # Local work files cleaned up
        downloads = work_dir / "downloads"
        leftover = list(downloads.rglob("*.*")) if downloads.exists() else []
        assert len(leftover) == 0


# ---------------------------------------------------------------------------
# Tests: upload lands next to source
# ---------------------------------------------------------------------------

class TestUploadVerification:
    """Verify uploaded files land next to source files in dataset1."""

    def test_results_appear_next_to_sources(self, dataset1, work_dir):
        """Process Artist_03/Album_1 — results appear in the same dirs."""
        mock = _make_mock(dataset1)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                artists=["Artist_03"],
                albums=["Album_1"],
                queue_size=8,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        result_path = item.local_path.with_suffix(".mir.json")
                        result_path.write_text(json.dumps({"ok": True}))
                        pipeline.submit_result(
                            item, result_path, f"{item.metadata['track']}.mir.json"
                        )

        # 5 results in Artist_03/Album_1/
        album_dir = dataset1 / "Artist_03" / "Album_1"
        results = list(album_dir.glob("*.mir.json"))
        assert len(results) == 5

        # Each result sits next to the original mp3
        for r in results:
            # r.name is e.g. "track_0.mir.json" — strip all suffixes
            track_name = r.name.split(".")[0]  # "track_0"
            original = album_dir / f"{track_name}_original.mp3"
            assert original.exists(), f"Source mp3 missing next to result: {r}"
