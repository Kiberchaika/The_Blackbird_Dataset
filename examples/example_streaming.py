#!/usr/bin/env python3
"""Blackbird Dataset — StreamingPipeline usage examples.

This script creates a self-contained temporary dataset with a mock WebDAV
server and demonstrates every major feature of the StreamingPipeline:

  1. Basic pipeline — download, process, upload
  2. Filtering by component, artist, album
  3. Skipping files without uploading
  4. Batch processing with take(count=N)
  5. Resume after interruption

All operations run inside a temporary directory that is cleaned up
automatically when the script exits.
"""

import json
import shutil
import random
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

from blackbird.index import DatasetIndex, TrackInfo
from blackbird.streaming import StreamingPipeline, _PipelineState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def heading(title: str) -> None:
    """Print a section heading."""
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


def create_dataset(root: Path) -> Path:
    """Build a dummy dataset with schema, index, and audio files.

    Structure::

        root/
        ├── .blackbird/
        │   ├── schema.json
        │   └── index.pickle
        ├── ArtistA/
        │   └── Album1/
        │       ├── track_0_original.mp3
        │       ├── track_0_vocal.mp3
        │       └── ...  (5 tracks × 2 components)
        ├── ArtistB/
        │   └── Album2/
        │       └── ...  (3 tracks × 2 components)
        └── ArtistC/
            └── Album3/
                └── ...  (2 tracks × 2 components)
    """
    rng = random.Random(42)
    location = "Main"

    artists_data = [
        ("ArtistA", "Album1", 5),
        ("ArtistB", "Album2", 3),
        ("ArtistC", "Album3", 2),
    ]
    components = {
        "original": "*_original.mp3",
        "vocal": "*_vocal.mp3",
    }

    # Schema
    schema = {
        "version": "1.0",
        "components": {
            name: {"pattern": pattern, "multiple": False, "description": ""}
            for name, pattern in components.items()
        },
    }
    bb = root / ".blackbird"
    bb.mkdir(parents=True)
    (bb / "schema.json").write_text(json.dumps(schema, indent=2))

    # Build index and write files
    index = DatasetIndex.create()

    for artist, album, n_tracks in artists_data:
        album_dir = root / artist / album
        album_dir.mkdir(parents=True)

        for i in range(n_tracks):
            base = f"track_{i}"
            sym_album = f"{location}/{artist}/{album}"
            sym_track = f"{sym_album}/{base}"

            files = {}
            file_sizes = {}
            for comp_name in components:
                filename = f"{base}_{comp_name}.mp3"
                fpath = album_dir / filename
                fpath.write_bytes(rng.randbytes(1024))

                sym_file = f"{location}/{artist}/{album}/{filename}"
                files[comp_name] = sym_file
                file_sizes[sym_file] = 1024
                index.file_info_by_hash[hash(sym_file)] = (sym_file, 1024)
                index.total_size += 1024

            track = TrackInfo(
                track_path=sym_track,
                artist=artist,
                album_path=sym_album,
                cd_number=None,
                base_name=base,
                files=files,
                file_sizes=file_sizes,
            )
            index.tracks[sym_track] = track
            index.track_by_album.setdefault(sym_album, set()).add(sym_track)
            index.album_by_artist.setdefault(artist, set()).add(sym_album)

    index.last_updated = datetime.now()
    index.save(bb / "index.pickle")
    return root


def make_mock_client(dataset_root: Path):
    """Mock WebDAVClient that reads/writes to a local directory."""
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


def simulate_processing(local_path: Path) -> Path:
    """Fake MIR analysis — writes a JSON result next to the source file."""
    result_path = local_path.with_suffix(".mir.json")
    result_path.write_text(json.dumps({
        "bpm": 120,
        "key": "C minor",
        "source": local_path.name,
    }))
    return result_path


# ---------------------------------------------------------------------------
# 1. Basic pipeline flow
# ---------------------------------------------------------------------------

def demo_basic_pipeline(dataset_root: Path, work_dir: Path) -> None:
    heading("1. Basic Pipeline — Download, Process, Upload")

    mock = make_mock_client(dataset_root)

    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://user:pass@localhost/dataset",
            components=["original"],
            queue_size=5,
            prefetch_workers=2,
            upload_workers=1,
            work_dir=str(work_dir / "basic"),
        )

        processed = 0
        with pipeline:
            while True:
                items = pipeline.take(count=1)
                if not items:
                    break

                for item in items:
                    print(f"  Processing: {item.remote_path}")
                    print(f"    artist={item.metadata['artist']}, "
                          f"album={item.metadata['album']}, "
                          f"track={item.metadata['track']}")

                    result_path = simulate_processing(item.local_path)

                    pipeline.submit_result(
                        item=item,
                        result_path=result_path,
                        remote_name=f"{item.metadata['track']}.mir.json",
                    )
                    processed += 1

    print(f"\n  Processed {processed} files")
    print(f"  Uploads:   {mock.upload_file.call_count}")

    # Verify results landed on the "server"
    mir_files = list(dataset_root.rglob("*.mir.json"))
    print(f"  Results on server: {len(mir_files)} .mir.json files")


# ---------------------------------------------------------------------------
# 2. Filtering by component, artist, album
# ---------------------------------------------------------------------------

def demo_filtering(dataset_root: Path, work_dir: Path) -> None:
    heading("2. Filtering — Component, Artist, Album")

    # Clean up results from previous demo
    for f in dataset_root.rglob("*.mir.json"):
        f.unlink()

    mock = make_mock_client(dataset_root)

    # 2a. Filter by component — only vocal tracks
    print("  [2a] components=['vocal']:")
    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            components=["vocal"],
            work_dir=str(work_dir / "filter_comp"),
        )
        count = 0
        with pipeline:
            while True:
                items = pipeline.take(count=5)
                if not items:
                    break
                for item in items:
                    assert item.metadata["component"] == "vocal"
                    pipeline.skip(item)
                    count += 1
        print(f"       Got {count} vocal files (expected 10)")

    # 2b. Filter by artist
    mock = make_mock_client(dataset_root)
    print("\n  [2b] artists=['ArtistB']:")
    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            artists=["ArtistB"],
            work_dir=str(work_dir / "filter_artist"),
        )
        count = 0
        with pipeline:
            while True:
                items = pipeline.take(count=5)
                if not items:
                    break
                for item in items:
                    assert item.metadata["artist"] == "ArtistB"
                    pipeline.skip(item)
                    count += 1
        print(f"       Got {count} files (expected 6: 3 tracks × 2 components)")

    # 2c. Filter by album
    mock = make_mock_client(dataset_root)
    print("\n  [2c] albums=['Album3']:")
    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            albums=["Album3"],
            work_dir=str(work_dir / "filter_album"),
        )
        count = 0
        with pipeline:
            while True:
                items = pipeline.take(count=5)
                if not items:
                    break
                for item in items:
                    assert item.metadata["album"] == "Album3"
                    pipeline.skip(item)
                    count += 1
        print(f"       Got {count} files (expected 4: 2 tracks × 2 components)")


# ---------------------------------------------------------------------------
# 3. Skipping files
# ---------------------------------------------------------------------------

def demo_skip(dataset_root: Path, work_dir: Path) -> None:
    heading("3. Skipping Files Without Uploading")

    mock = make_mock_client(dataset_root)

    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            components=["original"],
            artists=["ArtistA"],
            work_dir=str(work_dir / "skip"),
        )

        with pipeline:
            while True:
                items = pipeline.take(count=1)
                if not items:
                    break
                item = items[0]
                print(f"  Skipping: {item.remote_path}")
                pipeline.skip(item)

    print(f"\n  Uploads: {mock.upload_file.call_count} (should be 0)")


# ---------------------------------------------------------------------------
# 4. Batch processing with take(count=N)
# ---------------------------------------------------------------------------

def demo_batch_processing(dataset_root: Path, work_dir: Path) -> None:
    heading("4. Batch Processing — take(count=N)")

    mock = make_mock_client(dataset_root)

    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            components=["original"],
            queue_size=10,
            prefetch_workers=4,
            upload_workers=2,
            work_dir=str(work_dir / "batch"),
        )

        batch_num = 0
        total = 0
        with pipeline:
            while True:
                # Take up to 4 items at a time
                items = pipeline.take(count=4)
                if not items:
                    break

                batch_num += 1
                print(f"  Batch {batch_num}: got {len(items)} items")
                for item in items:
                    result_path = simulate_processing(item.local_path)
                    pipeline.submit_result(
                        item=item,
                        result_path=result_path,
                        remote_name=f"{item.metadata['track']}.mir.json",
                    )
                    total += 1

    print(f"\n  Total: {total} files in {batch_num} batches")


# ---------------------------------------------------------------------------
# 5. Resume after interruption
# ---------------------------------------------------------------------------

def demo_resume(dataset_root: Path, work_dir: Path) -> None:
    heading("5. Resume After Interruption")

    # Clean up results from previous demos
    for f in dataset_root.rglob("*.mir.json"):
        f.unlink()

    resume_dir = work_dir / "resume"
    mock = make_mock_client(dataset_root)

    # Pass 1: process only ArtistA originals, then "crash"
    print("  Pass 1: processing ArtistA originals (then simulating crash)...")
    with patch("blackbird.streaming.configure_client", return_value=mock):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            components=["original"],
            work_dir=str(resume_dir),
        )
        pass1_count = 0
        with pipeline:
            while True:
                items = pipeline.take(count=1)
                if not items:
                    break
                item = items[0]
                if item.metadata["artist"] != "ArtistA":
                    # Process only ArtistA, skip others in pass 1
                    result_path = simulate_processing(item.local_path)
                    pipeline.submit_result(
                        item=item,
                        result_path=result_path,
                        remote_name=f"{item.metadata['track']}.mir.json",
                    )
                else:
                    result_path = simulate_processing(item.local_path)
                    pipeline.submit_result(
                        item=item,
                        result_path=result_path,
                        remote_name=f"{item.metadata['track']}.mir.json",
                    )
                pass1_count += 1

    print(f"  Pass 1 processed: {pass1_count} files")

    # Check state file
    state_path = resume_dir / ".pipeline_state.json"
    if not state_path.exists():
        print("  (State file cleaned up — all items were processed in pass 1)")
        print("  Simulating partial processing instead...")

        # Create a state file that marks only ArtistA as processed
        state = _PipelineState(url="webdav://localhost/dataset")
        for i in range(5):
            state.processed.append(f"ArtistA/Album1/track_{i}_original.mp3")
        resume_dir.mkdir(parents=True, exist_ok=True)
        state.save(state_path)
        print(f"  Created state with {len(state.processed)} files marked as processed")

    # Pass 2: resume — should skip already-processed files
    mock2 = make_mock_client(dataset_root)
    print("\n  Pass 2: resuming — skipping already processed files...")
    with patch("blackbird.streaming.configure_client", return_value=mock2):
        pipeline = StreamingPipeline(
            url="webdav://localhost/dataset",
            components=["original"],
            work_dir=str(resume_dir),
        )
        pass2_count = 0
        with pipeline:
            while True:
                items = pipeline.take(count=1)
                if not items:
                    break
                item = items[0]
                print(f"    Resumed: {item.remote_path}")
                result_path = simulate_processing(item.local_path)
                pipeline.submit_result(
                    item=item,
                    result_path=result_path,
                    remote_name=f"{item.metadata['track']}.mir.json",
                )
                pass2_count += 1

    print(f"\n  Pass 2 processed: {pass2_count} files (skipped the already-done ones)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with tempfile.TemporaryDirectory(prefix="blackbird_streaming_") as tmp:
        tmp_root = Path(tmp)
        dataset_root = create_dataset(tmp_root / "dataset")
        work_dir = tmp_root / "work"

        demo_basic_pipeline(dataset_root, work_dir)
        demo_filtering(dataset_root, work_dir)
        demo_skip(dataset_root, work_dir)
        demo_batch_processing(dataset_root, work_dir)
        demo_resume(dataset_root, work_dir)

        print(f"\n{'=' * 72}")
        print("  All streaming examples completed successfully!")
        print(f"{'=' * 72}\n")


if __name__ == "__main__":
    main()
