"""Tests for StreamingPipeline."""

import pytest
import tempfile
import pickle
import json
import time
import threading
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

from blackbird.streaming import StreamingPipeline, PipelineItem, _PipelineState, _UploadTask
from blackbird.index import DatasetIndex, TrackInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_index(tracks_data):
    """Create a DatasetIndex with given tracks.

    tracks_data: list of dicts with keys:
        artist, album, base_name, components: {comp_name: filename}
    """
    idx = DatasetIndex.create()
    for t in tracks_data:
        artist = t["artist"]
        album = t["album"]
        base = t["base_name"]
        location = t.get("location", "Main")

        album_path = f"{location}/{artist}/{album}"
        track_path = f"{album_path}/{base}"

        files = {}
        file_sizes = {}
        for comp_name, filename in t["components"].items():
            sym = f"{location}/{artist}/{album}/{filename}"
            files[comp_name] = sym
            file_sizes[sym] = t.get("size", 1024)

        track = TrackInfo(
            track_path=track_path,
            artist=artist,
            album_path=album_path,
            cd_number=None,
            base_name=base,
            files=files,
            file_sizes=file_sizes,
        )
        idx.tracks[track_path] = track
        idx.track_by_album.setdefault(album_path, set()).add(track_path)
        idx.album_by_artist.setdefault(artist, set()).add(album_path)

        for sym, size in file_sizes.items():
            idx.file_info_by_hash[hash(sym)] = (sym, size)
            idx.total_size += size

    return idx


def _make_schema(components):
    """Create a minimal schema dict."""
    return {
        "version": "1.0",
        "components": {
            name: {"pattern": f"*_{name}.mp3", "multiple": False, "description": ""}
            for name in components
        },
    }


@pytest.fixture
def work_dir(tmp_path):
    return tmp_path / "pipeline_work"


@pytest.fixture
def index_with_tracks():
    """An index with 5 tracks, 2 components each."""
    tracks = []
    for i in range(5):
        tracks.append({
            "artist": "ArtistA",
            "album": "Album1",
            "base_name": f"track{i}",
            "components": {
                "original": f"track{i}_original.mp3",
                "vocal": f"track{i}_vocal.mp3",
            },
            "size": 2048,
        })
    return _make_index(tracks)


@pytest.fixture
def schema_data():
    return _make_schema(["original", "vocal"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _save_index_and_schema(work_dir, index, schema_data):
    """Save index and schema to work_dir so pipeline can load them."""
    work_dir.mkdir(parents=True, exist_ok=True)
    index_path = work_dir / "index.pickle"
    index.save(index_path)
    schema_path = work_dir / "schema.json"
    with open(schema_path, "w") as f:
        json.dump(schema_data, f)


def _mock_client(work_dir, index, schema_data):
    """Create a mock WebDAVClient that serves index and schema."""
    # Save source data to a separate dir to avoid same-file errors
    source_dir = work_dir.parent / "mock_source"
    _save_index_and_schema(source_dir, index, schema_data)

    mock = MagicMock()

    def download_side_effect(remote_path, local_path, **kwargs):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if remote_path == ".blackbird/index.pickle":
            import shutil
            shutil.copy(source_dir / "index.pickle", local_path)
            return True
        elif remote_path == ".blackbird/schema.json":
            import shutil
            shutil.copy(source_dir / "schema.json", local_path)
            return True
        else:
            # Simulate downloading a real file
            local_path.write_bytes(b"fake audio content " + remote_path.encode())
            return True

    mock.download_file = MagicMock(side_effect=download_side_effect)
    mock.upload_file = MagicMock(return_value=True)

    return mock


# ---------------------------------------------------------------------------
# Tests: PipelineState
# ---------------------------------------------------------------------------

class TestPipelineState:
    def test_save_and_load(self, tmp_path):
        path = tmp_path / "state.json"
        state = _PipelineState(url="webdav://host/data")
        state.processed = ["a/b/c.mp3"]
        state.pending_uploads = [{"local": "/tmp/x.json", "remote": "a/b/x.json"}]
        state.save(path)

        loaded = _PipelineState.load(path)
        assert loaded.url == "webdav://host/data"
        assert loaded.processed == ["a/b/c.mp3"]
        assert len(loaded.pending_uploads) == 1

    def test_load_missing_fields_uses_defaults(self, tmp_path):
        path = tmp_path / "state.json"
        with open(path, "w") as f:
            json.dump({"url": "webdav://x"}, f)
        loaded = _PipelineState.load(path)
        assert loaded.processed == []
        assert loaded.pending_uploads == []


# ---------------------------------------------------------------------------
# Tests: StreamingPipeline (unit)
# ---------------------------------------------------------------------------

class TestBuildWebdavUrl:
    def test_passthrough_webdav_url(self):
        url = StreamingPipeline._build_webdav_url("webdav://user:pass@host/path", None, None)
        assert url == "webdav://user:pass@host/path"

    def test_http_to_webdav_with_creds(self):
        url = StreamingPipeline._build_webdav_url("http://host:8080/data", "user", "pass")
        assert url == "webdav://user:pass@host:8080/data"

    def test_https_to_webdav_no_creds(self):
        url = StreamingPipeline._build_webdav_url("https://myhost/dataset", None, None)
        assert url == "webdav://myhost/dataset"


class TestBuildFileList:
    def test_builds_correct_list(self, work_dir, index_with_tracks, schema_data):
        """File list includes all component files from the index."""
        pipeline = StreamingPipeline(
            url="webdav://host/data",
            work_dir=str(work_dir),
        )
        pipeline._index = index_with_tracks
        pipeline._state = _PipelineState(url="webdav://host/data")
        # Write schema so _build_file_list can read it
        _save_index_and_schema(work_dir, index_with_tracks, schema_data)
        pipeline._build_file_list()

        # 5 tracks * 2 components = 10 files
        assert len(pipeline._file_list) == 10

    def test_component_filter(self, work_dir, index_with_tracks, schema_data):
        """Only requested components are included."""
        pipeline = StreamingPipeline(
            url="webdav://host/data",
            components=["original"],
            work_dir=str(work_dir),
        )
        pipeline._index = index_with_tracks
        pipeline._state = _PipelineState(url="webdav://host/data")
        _save_index_and_schema(work_dir, index_with_tracks, schema_data)
        pipeline._build_file_list()

        assert len(pipeline._file_list) == 5
        assert all(f["metadata"]["component"] == "original" for f in pipeline._file_list)

    def test_artist_filter(self, work_dir, schema_data):
        """Only requested artists are included."""
        tracks = [
            {"artist": "ArtistA", "album": "A1", "base_name": "t1",
             "components": {"original": "t1_original.mp3"}},
            {"artist": "ArtistB", "album": "B1", "base_name": "t2",
             "components": {"original": "t2_original.mp3"}},
        ]
        index = _make_index(tracks)

        pipeline = StreamingPipeline(
            url="webdav://host/data",
            artists=["ArtistA"],
            work_dir=str(work_dir),
        )
        pipeline._index = index
        pipeline._state = _PipelineState(url="webdav://host/data")
        _save_index_and_schema(work_dir, index, schema_data)
        pipeline._build_file_list()

        assert len(pipeline._file_list) == 1
        assert pipeline._file_list[0]["metadata"]["artist"] == "ArtistA"

    def test_skips_already_processed(self, work_dir, index_with_tracks, schema_data):
        """Files in state.processed are skipped."""
        pipeline = StreamingPipeline(
            url="webdav://host/data",
            components=["original"],
            work_dir=str(work_dir),
        )
        pipeline._index = index_with_tracks
        state = _PipelineState(url="webdav://host/data")
        # Mark first track as processed
        state.processed = ["ArtistA/Album1/track0_original.mp3"]
        pipeline._state = state
        _save_index_and_schema(work_dir, index_with_tracks, schema_data)
        pipeline._build_file_list()

        assert len(pipeline._file_list) == 4  # 5 - 1 skipped


# ---------------------------------------------------------------------------
# Tests: StreamingPipeline (integration with mocked WebDAV)
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    def test_full_pipeline_flow(self, work_dir, index_with_tracks, schema_data):
        """End-to-end: download -> take -> submit_result -> upload -> cleanup."""
        mock = _mock_client(work_dir, index_with_tracks, schema_data)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=5,
                prefetch_workers=2,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            with pipeline:
                processed_count = 0
                while True:
                    items = pipeline.take(count=2)
                    if not items:
                        break
                    for item in items:
                        # Simulate processing
                        result_path = item.local_path.with_suffix(".mir.json")
                        result_path.write_text('{"key": "value"}')

                        pipeline.submit_result(
                            item=item,
                            result_path=result_path,
                            remote_name=f"{item.metadata['track']}.mir.json",
                        )
                        processed_count += 1

            assert processed_count == 5
            assert mock.upload_file.call_count == 5

    def test_skip_does_not_upload(self, work_dir, index_with_tracks, schema_data):
        """Skipped items are not uploaded."""
        mock = _mock_client(work_dir, index_with_tracks, schema_data)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=5,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            with pipeline:
                items = pipeline.take(count=1)
                assert len(items) == 1
                pipeline.skip(items[0])

            # Only skip, no upload
            assert mock.upload_file.call_count == 0

    def test_resume_skips_processed(self, work_dir, index_with_tracks, schema_data):
        """Pipeline resumes and skips already-processed files."""
        mock = _mock_client(work_dir, index_with_tracks, schema_data)

        # Pre-create state with some files marked as processed
        work_dir.mkdir(parents=True, exist_ok=True)
        state = _PipelineState(url="webdav://host/data")
        state.processed = [
            "ArtistA/Album1/track0_original.mp3",
            "ArtistA/Album1/track1_original.mp3",
            "ArtistA/Album1/track2_original.mp3",
        ]
        state.save(work_dir / ".pipeline_state.json")

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=5,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            processed_count = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    for item in items:
                        result_path = item.local_path.with_suffix(".out")
                        result_path.write_text("data")
                        pipeline.submit_result(item, result_path, "result.json")
                        processed_count += 1

            # Only 2 remaining (5 total - 3 already processed)
            assert processed_count == 2

    def test_download_failure_does_not_block(self, work_dir, index_with_tracks, schema_data):
        """Failed downloads are logged but pipeline continues."""
        mock = _mock_client(work_dir, index_with_tracks, schema_data)
        source_dir = work_dir.parent / "mock_source"

        call_count = 0

        def flaky_download(remote_path, local_path, **kwargs):
            nonlocal call_count
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            if remote_path == ".blackbird/index.pickle":
                import shutil
                shutil.copy(source_dir / "index.pickle", local_path)
                return True
            elif remote_path == ".blackbird/schema.json":
                import shutil
                shutil.copy(source_dir / "schema.json", local_path)
                return True
            else:
                call_count += 1
                if call_count <= 3:  # First file fails all retries
                    return False
                local_path.write_bytes(b"ok")
                return True

        mock.download_file = MagicMock(side_effect=flaky_download)

        with patch("blackbird.streaming.configure_client", return_value=mock):
            pipeline = StreamingPipeline(
                url="webdav://host/data",
                components=["original"],
                queue_size=5,
                prefetch_workers=1,
                upload_workers=1,
                work_dir=str(work_dir),
            )

            items_received = 0
            with pipeline:
                while True:
                    items = pipeline.take(count=1)
                    if not items:
                        break
                    pipeline.skip(items[0])
                    items_received += 1

            # At least some items should have been received (not all 5 because first fails)
            assert items_received >= 1
            assert pipeline._failed_downloads >= 1


# ---------------------------------------------------------------------------
# Tests: Upload methods
# ---------------------------------------------------------------------------

class TestUploadWithRetry:
    """Tests for StreamingPipeline._upload_with_retry."""

    def _make_pipeline(self, work_dir):
        p = StreamingPipeline(url="webdav://host/data", work_dir=str(work_dir))
        p._client = MagicMock()
        return p

    def test_succeeds_on_first_attempt(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.return_value = True

        result = pipeline._upload_with_retry(Path("/tmp/f.json"), "a/b/f.json")

        assert result is True
        assert pipeline._client.upload_file.call_count == 1

    def test_succeeds_after_transient_failure(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.side_effect = [False, True]

        with patch("blackbird.streaming.time.sleep"):
            result = pipeline._upload_with_retry(Path("/tmp/f.json"), "a/b/f.json")

        assert result is True
        assert pipeline._client.upload_file.call_count == 2

    def test_succeeds_after_exception(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.side_effect = [ConnectionError("timeout"), True]

        with patch("blackbird.streaming.time.sleep"):
            result = pipeline._upload_with_retry(Path("/tmp/f.json"), "a/b/f.json")

        assert result is True
        assert pipeline._client.upload_file.call_count == 2

    def test_fails_after_max_retries(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.return_value = False

        with patch("blackbird.streaming.time.sleep"):
            result = pipeline._upload_with_retry(Path("/tmp/f.json"), "a/b/f.json")

        assert result is False
        assert pipeline._client.upload_file.call_count == 3  # MAX_RETRIES

    def test_aborts_on_shutdown(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._upload_shutdown.set()
        pipeline._client.upload_file.return_value = False

        result = pipeline._upload_with_retry(Path("/tmp/f.json"), "a/b/f.json")

        assert result is False


class TestProcessUploadTask:
    """Tests for StreamingPipeline._process_upload_task."""

    def _make_pipeline(self, work_dir):
        p = StreamingPipeline(url="webdav://host/data", work_dir=str(work_dir))
        p._client = MagicMock()
        p._state = _PipelineState(url="webdav://host/data")
        p._state_path = work_dir / ".pipeline_state.json"
        work_dir.mkdir(parents=True, exist_ok=True)
        p._state.save(p._state_path)
        return p

    def test_successful_upload_cleans_up_and_marks_processed(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.return_value = True

        # Create local files that should be cleaned up after upload
        src_file = work_dir / "downloads" / "ArtistA" / "Album1" / "track.mp3"
        src_file.parent.mkdir(parents=True, exist_ok=True)
        src_file.write_bytes(b"audio")

        result_file = work_dir / "results" / "track.mir.json"
        result_file.parent.mkdir(parents=True, exist_ok=True)
        result_file.write_text('{"bpm": 120}')

        item = PipelineItem(
            local_path=src_file,
            remote_path="ArtistA/Album1/track.mp3",
            metadata={"artist": "ArtistA", "album": "Album1", "track": "track", "component": "original"},
        )
        task = _UploadTask(item=item, result_path=result_file, remote_name="track.mir.json")

        pipeline._process_upload_task(task)

        # Upload was called with correct remote path
        pipeline._client.upload_file.assert_called_once_with(
            result_file, "ArtistA/Album1/track.mir.json"
        )
        # Local files cleaned up
        assert not src_file.exists()
        assert not result_file.exists()
        # State updated
        assert "ArtistA/Album1/track.mp3" in pipeline._state.processed
        assert pipeline._uploaded_count == 1
        assert pipeline._failed_uploads == 0

    def test_failed_upload_increments_failure_count(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.return_value = False

        src_file = work_dir / "downloads" / "track.mp3"
        src_file.parent.mkdir(parents=True, exist_ok=True)
        src_file.write_bytes(b"audio")

        result_file = work_dir / "results" / "track.mir.json"
        result_file.parent.mkdir(parents=True, exist_ok=True)
        result_file.write_text('{}')

        item = PipelineItem(
            local_path=src_file,
            remote_path="ArtistA/Album1/track.mp3",
            metadata={},
        )
        task = _UploadTask(item=item, result_path=result_file, remote_name="track.mir.json")

        with patch("blackbird.streaming.time.sleep"):
            pipeline._process_upload_task(task)

        assert pipeline._failed_uploads == 1
        assert pipeline._uploaded_count == 0
        # Local files NOT cleaned up on failure
        assert src_file.exists()
        assert result_file.exists()
        # Not marked as processed
        assert "ArtistA/Album1/track.mp3" not in pipeline._state.processed

    def test_removes_entry_from_pending_uploads(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._client.upload_file.return_value = True

        # Pre-populate pending uploads in state
        pipeline._state.pending_uploads = [
            {"local": "/tmp/track.mir.json", "remote": "ArtistA/Album1/track.mir.json"},
            {"local": "/tmp/other.json", "remote": "ArtistB/Album2/other.json"},
        ]
        pipeline._state.save(pipeline._state_path)

        src_file = work_dir / "track.mp3"
        src_file.write_bytes(b"audio")
        result_file = work_dir / "track.mir.json"
        result_file.write_text('{}')

        item = PipelineItem(
            local_path=src_file,
            remote_path="ArtistA/Album1/track.mp3",
            metadata={},
        )
        task = _UploadTask(item=item, result_path=result_file, remote_name="track.mir.json")

        pipeline._process_upload_task(task)

        # Only the matching entry removed; the other remains
        assert len(pipeline._state.pending_uploads) == 1
        assert pipeline._state.pending_uploads[0]["remote"] == "ArtistB/Album2/other.json"


class TestRemovePendingUpload:
    """Tests for StreamingPipeline._remove_pending_upload."""

    def _make_pipeline(self, work_dir):
        p = StreamingPipeline(url="webdav://host/data", work_dir=str(work_dir))
        p._state = _PipelineState(url="webdav://host/data")
        p._state_path = work_dir / ".pipeline_state.json"
        work_dir.mkdir(parents=True, exist_ok=True)
        p._state.save(p._state_path)
        return p

    def test_removes_matching_entry(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._state.pending_uploads = [
            {"local": "/tmp/a.json", "remote": "X/a.json"},
            {"local": "/tmp/b.json", "remote": "Y/b.json"},
        ]

        pipeline._remove_pending_upload("X/a.json")

        assert len(pipeline._state.pending_uploads) == 1
        assert pipeline._state.pending_uploads[0]["remote"] == "Y/b.json"

    def test_no_op_when_not_found(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._state.pending_uploads = [
            {"local": "/tmp/a.json", "remote": "X/a.json"},
        ]

        pipeline._remove_pending_upload("nonexistent/path.json")

        assert len(pipeline._state.pending_uploads) == 1

    def test_persists_to_disk(self, work_dir):
        pipeline = self._make_pipeline(work_dir)
        pipeline._state.pending_uploads = [
            {"local": "/tmp/a.json", "remote": "X/a.json"},
        ]

        pipeline._remove_pending_upload("X/a.json")

        # Reload from disk and verify
        reloaded = _PipelineState.load(pipeline._state_path)
        assert len(reloaded.pending_uploads) == 0
