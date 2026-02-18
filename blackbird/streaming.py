"""Streaming pipeline for processing remote datasets.

Downloads files from a WebDAV server into a queue, allows the user to
process them, uploads results back, and cleans up local files.
"""

from pathlib import Path
from typing import List, Optional, Dict
from dataclasses import dataclass, field
import logging
import threading
import queue
import json
import time
import signal
import os

from .sync import WebDAVClient, configure_client
from .index import DatasetIndex

logger = logging.getLogger(__name__)

# Retry / backoff constants
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # seconds
SERVER_UNAVAILABLE_TIMEOUT = 300  # 5 minutes


@dataclass
class PipelineItem:
    """A single item in the processing pipeline."""
    local_path: Path        # path to downloaded file in work_dir
    remote_path: str        # relative path on server (Artist/Album/track.mp3)
    metadata: Dict          # artist, album, track, component


@dataclass
class _UploadTask:
    """Internal: a result queued for upload."""
    item: PipelineItem
    result_path: Path
    remote_name: str


@dataclass
class _PipelineState:
    """Persistent state for resume support."""
    url: str
    processed: List[str] = field(default_factory=list)
    pending_uploads: List[Dict] = field(default_factory=list)

    def save(self, path: Path) -> None:
        data = {
            "url": self.url,
            "processed": self.processed,
            "pending_uploads": self.pending_uploads,
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: Path) -> '_PipelineState':
        with open(path, 'r') as f:
            data = json.load(f)
        state = cls(url=data["url"])
        state.processed = data.get("processed", [])
        state.pending_uploads = data.get("pending_uploads", [])
        return state


class StreamingPipeline:
    """Streaming pipeline: download -> process -> upload -> cleanup.

    Usage::

        pipeline = StreamingPipeline(
            url="webdav://user:pass@host/dataset",
            components=["original"],
            queue_size=8,
            prefetch_workers=4,
            upload_workers=2,
            work_dir="/tmp/blackbird_work",
        )

        with pipeline:
            while True:
                items = pipeline.take(count=4)
                if not items:
                    break
                for item in items:
                    result = process(item.local_path)
                    pipeline.submit_result(item, result, "track.mir.json")
    """

    def __init__(
        self,
        url: str,
        *,
        components: Optional[List[str]] = None,
        artists: Optional[List[str]] = None,
        albums: Optional[List[str]] = None,
        queue_size: int = 10,
        prefetch_workers: int = 4,
        upload_workers: int = 2,
        work_dir: str = "/tmp/blackbird_work",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        # Build the webdav:// URL with optional credentials
        self._raw_url = url
        self._webdav_url = self._build_webdav_url(url, username, password)

        self.components = components
        self.artists = artists
        self.albums = albums
        self.queue_size = queue_size
        self.prefetch_workers = prefetch_workers
        self.upload_workers = upload_workers
        self.work_dir = Path(work_dir)

        # Internal state
        self._client: Optional[WebDAVClient] = None
        self._index: Optional[DatasetIndex] = None
        self._file_list: List[Dict] = []  # [{remote_path, metadata}, ...]
        self._file_list_idx = 0

        # Queues
        self._download_queue: queue.Queue[Optional[PipelineItem]] = queue.Queue(maxsize=queue_size)
        self._upload_queue: queue.Queue[Optional[_UploadTask]] = queue.Queue()

        # Threading
        self._download_threads: List[threading.Thread] = []
        self._upload_threads: List[threading.Thread] = []
        self._file_list_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._shutdown_event = threading.Event()    # stops download workers
        self._upload_shutdown = threading.Event()   # stops upload workers (after drain)
        self._downloads_done = threading.Event()
        self._finished_workers = 0

        # State for resume
        self._state_path = self.work_dir / ".pipeline_state.json"
        self._state: Optional[_PipelineState] = None

        # Stats
        self._downloaded_count = 0
        self._uploaded_count = 0
        self._skipped_count = 0
        self._failed_downloads = 0
        self._failed_uploads = 0

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> 'StreamingPipeline':
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stop(interrupted=exc_type is not None)
        return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def take(self, count: int = 1) -> List[PipelineItem]:
        """Take *count* downloaded items from the queue.

        Blocks until *count* items are ready **or** the dataset is exhausted.
        Returns an empty list when there are no more items to process.
        """
        items: List[PipelineItem] = []
        while len(items) < count:
            try:
                item = self._download_queue.get(timeout=1.0)
            except queue.Empty:
                # Check if downloads are finished and queue is empty
                if self._downloads_done.is_set() and self._download_queue.empty():
                    break
                if self._shutdown_event.is_set():
                    break
                continue

            if item is None:
                # Sentinel: no more items will arrive
                break
            items.append(item)
        return items

    def submit_result(
        self,
        item: PipelineItem,
        result_path: Path,
        remote_name: str,
    ) -> None:
        """Queue a processing result for background upload.

        After successful upload both *item.local_path* and *result_path*
        will be deleted from disk.
        """
        result_path = Path(result_path)
        if not result_path.exists():
            logger.error(f"Result file not found: {result_path}")
            return

        task = _UploadTask(item=item, result_path=result_path, remote_name=remote_name)

        # Record pending upload in state for resume
        with self._state_lock:
            if self._state:
                remote_dir = str(Path(item.remote_path).parent)
                self._state.pending_uploads.append({
                    "local": str(result_path),
                    "remote": f"{remote_dir}/{remote_name}",
                })
                self._state.save(self._state_path)

        self._upload_queue.put(task)

    def skip(self, item: PipelineItem) -> None:
        """Skip a file — delete local source without uploading."""
        self._safe_delete(item.local_path)
        self._mark_processed(item.remote_path)

    # ------------------------------------------------------------------
    # Internals — startup / shutdown
    # ------------------------------------------------------------------

    def _start(self) -> None:
        """Initialize client, index, file list and start workers."""
        self.work_dir.mkdir(parents=True, exist_ok=True)

        # Connect
        logger.info(f"Connecting to {self._raw_url} ...")
        self._client = configure_client(self._webdav_url)

        # Download index
        logger.info("Downloading remote index ...")
        index_path = self.work_dir / "index.pickle"
        if not self._client.download_file(".blackbird/index.pickle", index_path):
            raise ConnectionError("Failed to download remote index.pickle")
        self._index = DatasetIndex.load(index_path)

        # Download schema (for component validation)
        schema_path = self.work_dir / "schema.json"
        if not self._client.download_file(".blackbird/schema.json", schema_path):
            raise ConnectionError("Failed to download remote schema.json")

        # Load / create state
        self._load_or_create_state()

        # Build file list
        self._build_file_list()
        logger.info(f"Files to process: {len(self._file_list)}")

        # Resume pending uploads from previous run
        self._resume_pending_uploads()

        # Start workers
        self._shutdown_event.clear()
        self._downloads_done.clear()

        for i in range(self.prefetch_workers):
            t = threading.Thread(target=self._download_worker, name=f"dl-{i}", daemon=True)
            t.start()
            self._download_threads.append(t)

        for i in range(self.upload_workers):
            t = threading.Thread(target=self._upload_worker, name=f"ul-{i}", daemon=True)
            t.start()
            self._upload_threads.append(t)

        logger.info(
            f"Pipeline started: {self.prefetch_workers} download workers, "
            f"{self.upload_workers} upload workers"
        )

    def _stop(self, interrupted: bool = False) -> None:
        """Shut down workers and clean up."""
        logger.info("Shutting down pipeline ...")

        # Stop download workers first (no more new items)
        self._shutdown_event.set()
        for t in self._download_threads:
            t.join(timeout=5.0)

        # Drain upload queue if not interrupted (uploads use _upload_shutdown)
        if not interrupted:
            logger.info("Waiting for pending uploads to finish ...")
            self._upload_queue.join()

        # Now signal upload workers to stop
        self._upload_shutdown.set()
        for _ in self._upload_threads:
            self._upload_queue.put(None)
        for t in self._upload_threads:
            t.join(timeout=30.0)

        # Save final state
        with self._state_lock:
            if self._state:
                self._state.save(self._state_path)

        # Clean up state file if everything is done
        if not interrupted and self._failed_uploads == 0 and self._failed_downloads == 0:
            if self._state_path.exists():
                self._state_path.unlink()
                logger.info("All items processed successfully — state file removed.")

        logger.info(
            f"Pipeline stopped. Downloaded: {self._downloaded_count}, "
            f"Uploaded: {self._uploaded_count}, "
            f"Skipped: {self._skipped_count}, "
            f"Failed DL: {self._failed_downloads}, "
            f"Failed UL: {self._failed_uploads}"
        )

    # ------------------------------------------------------------------
    # Internals — file list
    # ------------------------------------------------------------------

    def _build_file_list(self) -> None:
        """Build the list of files to download from the remote index."""
        assert self._index is not None

        already_processed = set()
        if self._state:
            already_processed = set(self._state.processed)

        import json as _json
        schema_data = {}
        schema_file = self.work_dir / "schema.json"
        if schema_file.exists():
            with open(schema_file, 'r') as f:
                schema_data = _json.load(f)

        # Resolve which components to download
        available_components = set(schema_data.get("components", {}).keys())
        if self.components:
            target_components = set(self.components) & available_components
            unknown = set(self.components) - available_components
            if unknown:
                logger.warning(f"Unknown components (ignored): {unknown}")
        else:
            target_components = available_components

        file_list: List[Dict] = []

        for track_path, track_info in self._index.tracks.items():
            # Artist filter
            if self.artists and not any(
                track_info.artist == a or track_info.artist.lower() == a.lower()
                for a in self.artists
            ):
                continue

            # Album filter
            if self.albums:
                album_name = track_info.album_path.split('/')[-1]
                if not any(
                    album_name == a or album_name.lower() == a.lower()
                    for a in self.albums
                ):
                    continue

            for comp_name, symbolic_file_path in track_info.files.items():
                if comp_name not in target_components:
                    continue

                # Strip location prefix to get the relative remote path
                parts = symbolic_file_path.split('/', 1)
                remote_path = parts[1] if len(parts) == 2 else symbolic_file_path

                if remote_path in already_processed:
                    self._skipped_count += 1
                    continue

                file_list.append({
                    "remote_path": remote_path,
                    "metadata": {
                        "artist": track_info.artist,
                        "album": track_info.album_path.split('/')[-1],
                        "track": track_info.base_name,
                        "component": comp_name,
                    },
                })

        self._file_list = file_list
        self._file_list_idx = 0

    def _next_file(self) -> Optional[Dict]:
        """Thread-safe: get next file dict from the list."""
        with self._file_list_lock:
            if self._file_list_idx >= len(self._file_list):
                return None
            entry = self._file_list[self._file_list_idx]
            self._file_list_idx += 1
            return entry

    # ------------------------------------------------------------------
    # Internals — download worker
    # ------------------------------------------------------------------

    def _download_worker(self) -> None:
        """Background thread: download files and put them in the queue."""
        while not self._shutdown_event.is_set():
            entry = self._next_file()
            if entry is None:
                # No more files for this worker.
                # Track how many workers have finished; only the last one sends sentinel.
                with self._file_list_lock:
                    self._finished_workers += 1
                    all_done = self._finished_workers >= self.prefetch_workers
                if all_done:
                    self._downloads_done.set()
                    self._download_queue.put(None)  # single sentinel
                return

            remote_path = entry["remote_path"]
            metadata = entry["metadata"]

            # Determine local path preserving directory structure
            local_path = self.work_dir / "downloads" / remote_path

            success = self._download_with_retry(remote_path, local_path)
            if success:
                item = PipelineItem(
                    local_path=local_path,
                    remote_path=remote_path,
                    metadata=metadata,
                )
                # Block until there's room in the queue (backpressure)
                while not self._shutdown_event.is_set():
                    try:
                        self._download_queue.put(item, timeout=1.0)
                        break
                    except queue.Full:
                        continue
                self._downloaded_count += 1
            else:
                self._failed_downloads += 1
                logger.error(f"Failed to download after retries: {remote_path}")

    def _download_with_retry(self, remote_path: str, local_path: Path) -> bool:
        """Download a file with retry logic."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(MAX_RETRIES):
            if self._shutdown_event.is_set():
                return False
            try:
                success = self._client.download_file(remote_path, local_path)
                if success:
                    return True
            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1}/{MAX_RETRIES} failed for {remote_path}: {e}")

            if attempt < MAX_RETRIES - 1:
                backoff = RETRY_BACKOFF_BASE ** attempt
                time.sleep(backoff)

        return False

    # ------------------------------------------------------------------
    # Internals — upload worker
    # ------------------------------------------------------------------

    def _upload_worker(self) -> None:
        """Background thread: upload results and clean up local files."""
        while True:
            try:
                task = self._upload_queue.get(timeout=1.0)
            except queue.Empty:
                if self._upload_shutdown.is_set():
                    return
                continue

            if task is None:
                self._upload_queue.task_done()
                return  # Shutdown sentinel

            try:
                self._process_upload_task(task)
            except Exception as e:
                logger.error(f"Unexpected error in upload worker: {e}")
            finally:
                self._upload_queue.task_done()

    def _process_upload_task(self, task: _UploadTask) -> None:
        """Handle a single upload task with retries."""
        remote_dir = str(Path(task.item.remote_path).parent)
        remote_result_path = f"{remote_dir}/{task.remote_name}"

        success = self._upload_with_retry(task.result_path, remote_result_path)
        if success:
            self._uploaded_count += 1

            # Clean up local files
            self._safe_delete(task.result_path)
            self._safe_delete(task.item.local_path)

            # Mark processed and remove from pending uploads
            self._mark_processed(task.item.remote_path)
            self._remove_pending_upload(remote_result_path)
        else:
            self._failed_uploads += 1
            logger.error(f"Failed to upload after retries: {remote_result_path}")

    def _upload_with_retry(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file with retry logic."""
        for attempt in range(MAX_RETRIES):
            if self._upload_shutdown.is_set():
                return False
            try:
                success = self._client.upload_file(local_path, remote_path)
                if success:
                    return True
            except Exception as e:
                logger.warning(f"Upload attempt {attempt + 1}/{MAX_RETRIES} failed for {remote_path}: {e}")

            if attempt < MAX_RETRIES - 1:
                backoff = RETRY_BACKOFF_BASE ** attempt
                time.sleep(backoff)

        return False

    # ------------------------------------------------------------------
    # Internals — state management
    # ------------------------------------------------------------------

    def _load_or_create_state(self) -> None:
        """Load existing state or create a fresh one."""
        if self._state_path.exists():
            try:
                self._state = _PipelineState.load(self._state_path)
                logger.info(
                    f"Resumed state: {len(self._state.processed)} processed, "
                    f"{len(self._state.pending_uploads)} pending uploads"
                )
                return
            except Exception as e:
                logger.warning(f"Failed to load state, starting fresh: {e}")

        self._state = _PipelineState(url=self._raw_url)
        self._state.save(self._state_path)

    def _mark_processed(self, remote_path: str) -> None:
        """Mark a remote path as fully processed in state."""
        with self._state_lock:
            if self._state and remote_path not in self._state.processed:
                self._state.processed.append(remote_path)
                self._state.save(self._state_path)

    def _remove_pending_upload(self, remote_result_path: str) -> None:
        """Remove a completed upload from the pending list."""
        with self._state_lock:
            if self._state:
                self._state.pending_uploads = [
                    p for p in self._state.pending_uploads
                    if p.get("remote") != remote_result_path
                ]
                self._state.save(self._state_path)

    def _resume_pending_uploads(self) -> None:
        """Re-queue any pending uploads from a previous run."""
        if not self._state or not self._state.pending_uploads:
            return

        logger.info(f"Resuming {len(self._state.pending_uploads)} pending uploads ...")
        remaining = []
        for entry in self._state.pending_uploads:
            local = Path(entry["local"])
            remote = entry["remote"]
            if local.exists():
                # We need a PipelineItem for the upload task; reconstruct minimal metadata
                item = PipelineItem(
                    local_path=local.parent / "source_placeholder",  # source may be gone
                    remote_path=str(Path(remote).parent / "placeholder"),
                    metadata={},
                )
                task = _UploadTask(item=item, result_path=local, remote_name=Path(remote).name)
                self._upload_queue.put(task)
                remaining.append(entry)
            else:
                logger.warning(f"Pending upload file missing, skipping: {local}")

        with self._state_lock:
            self._state.pending_uploads = remaining
            self._state.save(self._state_path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_delete(path: Path) -> None:
        """Delete a file if it exists, suppressing errors."""
        try:
            if path.exists():
                path.unlink()
        except OSError as e:
            logger.warning(f"Failed to delete {path}: {e}")

    @staticmethod
    def _build_webdav_url(url: str, username: Optional[str], password: Optional[str]) -> str:
        """Build a webdav:// URL, injecting credentials if provided."""
        # If already a webdav:// URL, return as-is
        if url.startswith("webdav://"):
            return url

        # Convert https:// or http:// to webdav://
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        path = parsed.path or "/"

        if username and password:
            return f"webdav://{username}:{password}@{host}{port}{path}"
        elif parsed.username and parsed.password:
            return f"webdav://{parsed.username}:{parsed.password}@{host}{port}{path}"
        else:
            return f"webdav://{host}{port}{path}"
