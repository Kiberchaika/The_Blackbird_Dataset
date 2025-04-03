from pathlib import Path
from typing import List, Optional, Dict, Set, DefaultDict, Tuple, Any
import logging
from enum import Enum
from dataclasses import dataclass, field
from webdav3.client import Client
from tqdm import tqdm
import os
from urllib.parse import urlparse, quote
import webdav3.client as webdav
import fnmatch
from difflib import get_close_matches
import click
from collections import defaultdict
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

# Import colorama for cross-platform colored terminal output
try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)  # Initialize colorama with autoreset
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Create dummy color classes if colorama is not available
    class DummyColors:
        def __getattr__(self, name):
            return ""
    Fore = DummyColors()
    Back = DummyColors()
    Style = DummyColors()

from .schema import DatasetComponentSchema
from .index import DatasetIndex
from .dataset import Dataset # Ensure Dataset is imported
from .operations import (
    create_operation_state,
    load_operation_state, # Add load_operation_state
    update_operation_state_file,
    delete_operation_state,
    OperationStatus,
    OperationState # Add OperationState type
)
from .locations import resolve_symbolic_path # Add resolve_symbolic_path

logger = logging.getLogger(__name__)

class SyncState(Enum):
    """Sync state for a file."""
    PENDING = "pending"
    SYNCED = "synced"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class ProfilingStats:
    """Statistics for profiling operations."""
    # Dictionary to store timing data for different operations
    operation_times: DefaultDict[str, list] = field(default_factory=lambda: defaultdict(list))
    
    def add_timing(self, operation: str, time_ns: int):
        """Add timing data for an operation."""
        self.operation_times[operation].append(time_ns)
    
    def get_summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary statistics."""
        summary = {}
        total_time = sum(sum(times) for times in self.operation_times.values())
        
        for operation, times in self.operation_times.items():
            op_total = sum(times)
            count = len(times)
            summary[operation] = {
                'total_ms': op_total / 1_000_000,  # Convert ns to ms
                'avg_ms': (op_total / count) / 1_000_000 if count > 0 else 0,
                'calls': count,
                'percentage': (op_total / total_time * 100) if total_time > 0 else 0
            }
            
        return summary

@dataclass
class SyncStats:
    """Statistics for a sync operation."""
    total_files: int = 0
    synced_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    total_size: int = 0
    synced_size: int = 0
    downloaded_files: int = 0
    downloaded_size: int = 0
    profiling: Optional[ProfilingStats] = None

    def enable_profiling(self):
        """Enable profiling for this sync operation."""
        self.profiling = ProfilingStats()

class WebDAVClient:
    """WebDAV client for remote dataset operations."""
    
    def __init__(self, url: str, use_http2: bool = False, connection_pool_size: int = 10):
        """Initialize WebDAV client.
        
        Args:
            url: WebDAV server URL with optional credentials
                Format: webdav://[user:pass@]host[:port]/path
            use_http2: Whether to use HTTP/2 for connections
            connection_pool_size: Size of the connection pool
        """
        parsed = urlparse(url)
        if parsed.scheme != 'webdav':
            raise ValueError("URL must use webdav:// scheme")
            
        # Extract credentials if present
        username = None
        password = None
        if '@' in parsed.netloc:
            auth, host = parsed.netloc.split('@')
            username, password = auth.split(':')
        else:
            host = parsed.netloc
            
        # Configure client options
        self.base_url = f"http://{host}"
        options = {
            'webdav_hostname': self.base_url,
            'webdav_root': parsed.path or '/',
        }
        
        if username and password:
            options['webdav_login'] = username
            options['webdav_password'] = password
            
        # Initialize standard WebDAV client
        self.client = Client(options)
        
        # Set up HTTP/2 client if requested and available
        self.use_http2 = use_http2 and HTTPX_AVAILABLE
        self.http2_client = None
        if self.use_http2 and HTTPX_AVAILABLE:
            self.http2_client = httpx.Client(http2=True)
            if username and password:
                self.http2_client.auth = (username, password)
        
        # Set up connection pooling
        self.connection_pool_size = connection_pool_size
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1)
        adapter = HTTPAdapter(
            pool_connections=connection_pool_size,
            pool_maxsize=connection_pool_size,
            max_retries=retries
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        if username and password:
            self.session.auth = (username, password)
    
    def _encode_url_path(self, path: str) -> str:
        """Properly encode URL path with special characters.
        
        Args:
            path: Path to encode
            
        Returns:
            URL-encoded path with / preserved
        """
        # Split path by / and encode each part separately
        parts = path.split('/')
        encoded_parts = [quote(part, safe='') for part in parts]
        return '/'.join(encoded_parts)
    
    def download_file(self, remote_path: str, local_path: Path, file_size: int = None, profiling: Optional[ProfilingStats] = None) -> bool:
        """Download a file from the WebDAV server.
        
        Args:
            remote_path: Path to the file on the server
            local_path: Local path to save the file
            file_size: Expected size of the file in bytes (optional, for interface compatibility)
            profiling: Optional profiling stats object
            
        Returns:
            True if the download was successful, False otherwise
        """
        try:
            start_download_total = time.time_ns() if profiling else 0
            
            # Convert local_path to Path if it's a string
            if isinstance(local_path, str):
                local_path = Path(local_path)
            
            # Create parent directory if it doesn't exist
            start_mkdir = time.time_ns() if profiling else 0
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if profiling:
                profiling.add_timing('mkdir', time.time_ns() - start_mkdir)
            
            # Properly encode the remote path for URL
            encoded_path = self._encode_url_path(remote_path)
            
            # Use HTTP/2 client if available and enabled
            if self.use_http2 and self.http2_client:
                start_http_setup = time.time_ns() if profiling else 0
                url = f"{self.base_url}/{encoded_path.lstrip('/')}"
                if profiling:
                    profiling.add_timing('http_setup', time.time_ns() - start_http_setup)
                
                start_http_request = time.time_ns() if profiling else 0
                with self.http2_client.stream('GET', url) as response:
                    if profiling:
                        profiling.add_timing('http_request', time.time_ns() - start_http_request)
                    
                    if response.status_code == 200:
                        start_file_write = time.time_ns() if profiling else 0
                        with open(local_path, 'wb') as f:
                            for chunk in response.iter_bytes(chunk_size=8192):
                                f.write(chunk)
                        if profiling:
                            profiling.add_timing('file_write', time.time_ns() - start_file_write)
                            profiling.add_timing('http2_download_total', time.time_ns() - start_download_total)
                        return True
                    else:
                        # Only log the first few 404s, then suppress them
                        if response.status_code != 404 or not hasattr(self, '_404_count') or self._404_count < 5:
                            if not hasattr(self, '_404_count'):
                                self._404_count = 0
                            if response.status_code == 404:
                                self._404_count += 1
                                if self._404_count == 5:
                                    logger.error("Suppressing further 404 error messages...")
                            logger.error(f"HTTP/2 download failed with status {response.status_code}: {url}")
                        return False
            
            # Use connection pooling if HTTP/2 is not available
            elif self.connection_pool_size > 0:
                start_http_setup = time.time_ns() if profiling else 0
                url = f"{self.base_url}/{encoded_path.lstrip('/')}"
                if profiling:
                    profiling.add_timing('http_setup', time.time_ns() - start_http_setup)
                
                start_http_request = time.time_ns() if profiling else 0
                with self.session.get(url, stream=True) as response:
                    if profiling:
                        profiling.add_timing('http_request', time.time_ns() - start_http_request)
                    
                    if response.status_code == 200:
                        start_file_write = time.time_ns() if profiling else 0
                        with open(local_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        if profiling:
                            profiling.add_timing('file_write', time.time_ns() - start_file_write)
                            profiling.add_timing('http_download_total', time.time_ns() - start_download_total)
                        return True
                    else:
                        # Only log the first few 404s, then suppress them
                        if response.status_code != 404 or not hasattr(self, '_404_count') or self._404_count < 5:
                            if not hasattr(self, '_404_count'):
                                self._404_count = 0
                            if response.status_code == 404:
                                self._404_count += 1
                                if self._404_count == 5:
                                    logger.error("Suppressing further 404 error messages...")
                            logger.error(f"HTTP download failed with status {response.status_code}: {url}")
                        return False
            
            # Fall back to standard WebDAV client
            else:
                start_webdav_download = time.time_ns() if profiling else 0
                # The standard WebDAV client may handle URL encoding differently
                # We'll need to test if it needs the encoded path or the original path
                try:
                    self.client.download_sync(remote_path=remote_path, local_path=str(local_path))
                except Exception as e:
                    # If the standard client fails, try with encoded path
                    if "#" in remote_path or "?" in remote_path or "+" in remote_path:
                        logger.debug(f"Standard download failed, trying with encoded path: {encoded_path}")
                        self.client.download_sync(remote_path=encoded_path, local_path=str(local_path))
                    else:
                        raise e
                
                if profiling:
                    profiling.add_timing('webdav_download_total', time.time_ns() - start_webdav_download)
                    profiling.add_timing('download_total', time.time_ns() - start_download_total)
                return True
                    
        except Exception as e:
            # Limit logging of common errors
            error_msg = str(e)
            if '404' not in error_msg or not hasattr(self, '_error_count') or self._error_count < 5:
                if not hasattr(self, '_error_count'):
                    self._error_count = 0
                self._error_count += 1
                if self._error_count == 5:
                    logger.error("Suppressing further error messages...")
            logger.error(f"Failed to download {remote_path}: {e}")
            return False
    
    def check_connection(self) -> bool:
        """Check if the connection to the WebDAV server is working."""
        return self.client.check_connection()
    
    def __getattr__(self, name):
        """Delegate method calls to the underlying WebDAV client."""
        return getattr(self.client, name)

class DatasetSync:
    """Manages dataset synchronization."""
    
    def __init__(self, dataset: Dataset):
        """Initialize DatasetSync.
        
        Args:
            dataset: The Dataset instance to sync.
        """
        if not isinstance(dataset, Dataset):
            raise TypeError("dataset must be an instance of the Dataset class.")
        self.dataset = dataset
        # Ensure dataset path exists and .blackbird subdir exists
        self.blackbird_dir = self.dataset.path / ".blackbird"
        self.blackbird_dir.mkdir(exist_ok=True)
        
        # Reset 404 counter for each instance
        self._404_count = 0
        
        # Load schema and index directly from the dataset object
        self.schema = self.dataset.schema 
        self.index = self.dataset.index
        
        # Remove redundant loading from paths
        # self.blackbird_dir = self.local_path / ".blackbird"
        # self.schema_path = self.blackbird_dir / "schema.json"
        # self.index_path = self.blackbird_dir / "index.pickle"
        # 
        # if self.schema_path.exists():
        #     self.schema = DatasetComponentSchema.load(self.schema_path)
        # else:
        #     raise ValueError(f"Schema not found at {self.schema_path}")
        #     
        # if self.index_path.exists():
        #     self.index = DatasetIndex.load(self.index_path)
        # else:
        #     raise ValueError(f"Index not found at {self.index_path}")
    
    def configure_client(self, webdav_url: str, username: str, password: str, 
                         use_http2: bool = False, connection_pool_size: int = 10) -> WebDAVClient:
        """Configure WebDAV client.
        
        Args:
            webdav_url: WebDAV server URL
            username: WebDAV username
            password: WebDAV password
            use_http2: Whether to use HTTP/2 for connections
            connection_pool_size: Size of the connection pool
            
        Returns:
            Configured WebDAV client
        """
        options = {
            'webdav_hostname': webdav_url,
            'webdav_login': username,
            'webdav_password': password,
            'disable_check': True
        }
        client = WebDAVClient(
            f"webdav://{username}:{password}@{urlparse(webdav_url).netloc}",
            use_http2=use_http2,
            connection_pool_size=connection_pool_size
        )
        return client
    
    def _download_file(self, client: Any, remote_path: str, local_path: Path, file_size: int, profiling: dict | None) -> Tuple[bool, int]:
        """Download a single file with profiling.
        
        Args:
            client: WebDAV client instance.
            remote_path: Path to file on remote server (relative to dataset root).
            local_path: Absolute local path to save the file.
            file_size: Expected size of the file in bytes.
            profiling: Dictionary to store profiling data (optional).
            
        Returns:
            Tuple (success_flag, downloaded_size)
        """
        start_time = time.time() if profiling is not None else None
        downloaded_size = 0
        
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Pass file_size to the client's download method
            success = client.download_file(
                remote_path=remote_path, 
                local_path=local_path,
                file_size=file_size,  # Pass the file_size here
                profiling=profiling # Pass profiling dict if enabled
            )
            
            if success:
                downloaded_size = file_size # Assume full download if success reported
            
            return success, downloaded_size
        except Exception as e:
            logger.error(f"Failed to sync {remote_path}: {e}")
            # Log traceback for more details on unexpected errors
            if not isinstance(e, (FileNotFoundError, IOError)): # Avoid noisy tracebacks for common errors
                logger.debug(f"Traceback for {remote_path} failure:", exc_info=True)
            return False, 0
        finally:
            if profiling is not None and start_time is not None:
                end_time = time.time()
                profiling[remote_path] = end_time - start_time
    
    def sync(
        self,
        client: Any, # Should be WebDAVClient, but use Any for flexibility if mocks are used
        components: List[str],
        artists: Optional[List[str]] = None,
        albums: Optional[List[str]] = None, # Currently unused filter, but keep for future
        missing_component: Optional[str] = None,
        resume: bool = True,
        enable_profiling: bool = False,
        parallel: int = 1,
        use_http2: bool = False, # Passed to WebDAVClient if needed, not directly used here
        connection_pool_size: int = 10, # Passed to WebDAVClient if needed
        target_location_name: str = "Main"
    ) -> SyncStats:
        """Synchronize the dataset from a remote WebDAV source.

        Args:
            client: WebDAV client instance.
            components: List of component names to synchronize.
            artists: Optional list of artist names/patterns to synchronize.
            albums: Optional list of album names/patterns to synchronize (relative to artist).
            missing_component: Only sync tracks that are missing this specific component.
            resume: Whether to resume synchronization (skip existing files). Defaults to True.
            enable_profiling: Whether to enable performance profiling.
            parallel: Number of parallel download threads.
            use_http2: Whether the client should attempt to use HTTP/2.
            connection_pool_size: Size of the connection pool for the client.
            target_location_name: The name of the local location to sync files to.

        Returns:
            SyncStats object with synchronization statistics.
        """
        start_sync = time.time_ns()
        stats = SyncStats()
        if enable_profiling:
            stats.enable_profiling()
            profiling = stats.profiling
        else:
            profiling = None

        try:
            # 0. Validate target location
            start_val_loc = time.time_ns() if profiling else 0
            if target_location_name not in self.dataset.locations.get_all_locations():
                raise ValueError(f"Target location '{target_location_name}' not found in local configuration.")
            target_location_path = self.dataset.locations.get_location_path(target_location_name)
            if not target_location_path or not target_location_path.is_dir():
                 raise ValueError(f"Target location path for '{target_location_name}' is invalid or not a directory: {target_location_path}")
            if profiling: profiling.add_timing('validate_target_location', time.time_ns() - start_val_loc)


            # 1. Load Remote Index (Assume client provides a method like get_index())
            # In a real scenario, client.get_index() would fetch and load the remote index
            # For now, assume the remote index is somehow available via the client or loaded externally.
            # Let's simulate this by assuming the client has an index attribute for now.
            start_load_idx = time.time_ns() if profiling else 0
            if not hasattr(client, 'get_index') or not callable(client.get_index):
                 logger.warning("WebDAV client does not have a 'get_index' method. Sync might not work as expected.")
                 # Attempt to load local index as a fallback or raise error?
                 # For now, assume local index IS the remote index for testing purposes if needed.
                 # raise NotImplementedError("Client must provide a get_index method")
                 # Using local index as placeholder remote index:
                 remote_index = self.dataset.index # Placeholder!
            else:
                remote_index = client.get_index()

            if not remote_index or not isinstance(remote_index, DatasetIndex):
                 raise ValueError("Failed to load or obtain a valid remote DatasetIndex.")
                 
            # Create symbolic_path -> hash lookup from the remote index
            symbolic_path_to_hash: Dict[str, int] = {
                info[0]: h for h, info in remote_index.file_info_by_hash.items()
            }
                 
            if profiling: profiling.add_timing('load_remote_index', time.time_ns() - start_load_idx)


            # 2. Identify files to sync based on filters
            start_filter = time.time_ns() if profiling else 0
            files_to_sync: Dict[str, int] = {} # symbolic_remote_path -> size

            # Filter by component first (mandatory)
            component_patterns = {}
            remote_schema = client.get_schema() # Assume client provides schema
            if not remote_schema:
                 raise ValueError("Failed to load remote schema.")
            for comp_name in components:
                if comp_name not in remote_schema.schema['components']:
                    # Check for close matches
                    available_comps = list(remote_schema.schema['components'].keys())
                    matches = get_close_matches(comp_name, available_comps, n=1, cutoff=0.6)
                    suggestion = f" Did you mean '{matches[0]}'?" if matches else ""
                    raise ValueError(f"Component '{comp_name}' not found in remote schema.{suggestion}")
                component_patterns[comp_name] = remote_schema.schema['components'][comp_name]['pattern']

            # Pre-filter tracks if missing_component is specified
            tracks_missing_component: Optional[Set[str]] = None
            if missing_component:
                if missing_component not in remote_schema.schema['components']:
                     raise ValueError(f"Missing component filter '{missing_component}' not found in remote schema.")
                tracks_missing_component = set()
                for track_path, track_info in remote_index.tracks.items():
                    if missing_component not in track_info.files:
                        tracks_missing_component.add(track_path)
                if not tracks_missing_component:
                    logger.info(f"No tracks found missing component '{missing_component}'. Sync will likely be empty.")
                    # return stats # Exit early if no tracks match the primary filter


            # Iterate through all tracks in the remote index
            for symbolic_track_path, track_info in remote_index.tracks.items():
                # Apply missing_component filter
                if tracks_missing_component is not None and symbolic_track_path not in tracks_missing_component:
                    continue

                # Apply artist filter (if provided)
                if artists:
                    # Use fnmatch for glob pattern matching on artist name
                    if not any(fnmatch.fnmatch(track_info.artist, pattern) for pattern in artists):
                        continue

                # Apply album filter (if provided) - requires symbolic album path matching
                # TODO: Implement album filtering logic if needed

                # Check desired components for this track
                for comp_name, symbolic_file_path in track_info.files.items():
                    if comp_name in component_patterns: # Check if this is one of the components we want
                         if symbolic_file_path in track_info.file_sizes:
                             files_to_sync[symbolic_file_path] = track_info.file_sizes[symbolic_file_path]
                         else:
                             logger.warning(f"File size missing for {symbolic_file_path} in remote index. Skipping.")

            stats.total_files = len(files_to_sync)
            stats.total_size = sum(files_to_sync.values())
            
            # Get list of hashes for the state file
            file_hashes_to_process = [
                symbolic_path_to_hash[p] for p in files_to_sync if p in symbolic_path_to_hash
            ]
            missing_hashes = [p for p in files_to_sync if p not in symbolic_path_to_hash]
            if missing_hashes:
                logger.warning(f"Could not find hashes for {len(missing_hashes)} files in the index. These won't be tracked in the state file.")
                # Optionally, raise an error or handle differently

            if profiling: profiling.add_timing('filter_files', time.time_ns() - start_filter)

            if not files_to_sync:
                logger.info("No files match the specified criteria for synchronization.")
                return stats

            # --- Operation State File Handling ---
            state_file_path = None
            try:
                start_state_create = time.time_ns() if profiling else 0
                # Ensure .blackbird directory exists (DatasetSync constructor should do this)
                self.blackbird_dir.mkdir(exist_ok=True)
                state_file_path = create_operation_state(
                    blackbird_dir=self.blackbird_dir,
                    operation_type="sync",
                    source=client.base_url + client.client.options.get('webdav_root', '/'), # Reconstruct source URL
                    target_location=target_location_name,
                    file_hashes=file_hashes_to_process,
                    components=components
                )
                if profiling: profiling.add_timing('create_state_file', time.time_ns() - start_state_create)
            except Exception as e:
                logger.error(f"Failed to create operation state file: {e}. Sync cannot proceed with state tracking.")
                # Decide how to proceed: continue without state tracking, or fail?
                # For now, let's log and continue, but state updates will fail later.
                # A more robust approach might be to raise the error here.
                # raise # Optionally re-raise to halt execution
                state_file_path = None # Ensure it's None if creation failed

            # --- End Operation State File Handling ---


            logger.info(f"Identified {stats.total_files} files ({self.dataset.format_size(stats.total_size)}) to potentially sync to location '{target_location_name}'.")


            # 3. Prepare for parallel download
            batch_size = max(1, stats.total_files // (parallel * 10)) # Heuristic for batch size
            file_list = list(files_to_sync.items())
            batches = [file_list[i:i + batch_size] for i in range(0, len(file_list), batch_size)]
            num_batches = len(batches)

            pbar_files = tqdm(total=stats.total_files, unit='file', desc="Syncing files", colour="green")
            pbar_bytes = tqdm(total=stats.total_size, unit='B', unit_scale=True, desc="Syncing size", colour="blue")

            processed_files_count = 0
            processed_bytes_count = 0

            # Inner function to process a batch of files
            def process_batch(batch_id, batch_files):
                nonlocal processed_files_count, processed_bytes_count
                batch_stats = SyncStats()
                batch_results = [] # Store (symbolic_path, status, size, error_msg)

                # Inner function to process a single file within the batch
                def _process_file(symbolic_remote_path: str, file_size: int):
                    file_status: SyncState = SyncState.PENDING
                    error_message: Optional[str] = None
                    downloaded_this_file = False
                    
                    # --- Resolve local path ---
                    start_resolve = time.time_ns() if profiling else 0
                    try:
                        # Strip location prefix for remote path used in download
                        location_prefix = symbolic_remote_path.split('/', 1)[0] + '/'
                        relative_remote_path = symbolic_remote_path[len(location_prefix):]
                        
                        # Use target location and relative path to form local path
                        local_file_path = target_location_path / relative_remote_path
                    except Exception as e:
                         logger.error(f"Error resolving path for {symbolic_remote_path}: {e}")
                         error_message = f"Path resolution error: {e}"
                         file_status = SyncState.FAILED
                         # --- State Update ---
                         file_hash = symbolic_path_to_hash.get(symbolic_remote_path)
                         if state_file_path and file_hash is not None:
                              update_operation_state_file(state_file_path, file_hash, f"failed: {error_message}")
                         # --- End State Update ---
                         return file_status, 0, error_message, downloaded_this_file
                    if profiling: profiling.add_timing('resolve_local_path', time.time_ns() - start_resolve)
                    # --- End Resolve local path ---
                    
                    # Check if file exists locally and if resume is enabled
                    start_check_local = time.time_ns() if profiling else 0
                    skip_file = False
                    if resume and local_file_path.exists():
                        local_size = local_file_path.stat().st_size
                        if local_size == file_size:
                            file_status = SyncState.SKIPPED
                            skip_file = True
                        else:
                            logger.warning(f"Local file size mismatch for {local_file_path} (local: {local_size}, remote: {file_size}). Re-downloading.")
                    if profiling: profiling.add_timing('check_local_file', time.time_ns() - start_check_local)

                    # Download if not skipping
                    if not skip_file:
                        download_successful = False
                        try:
                            # Pass profiling object if enabled
                            prof = profiling if enable_profiling else None
                            start_download = time.time_ns() if profiling else 0
                            
                            # Ensure parent directory exists right before download
                            local_file_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            download_successful = client.download_file(
                                remote_path=relative_remote_path, # Use relative path for download
                                local_path=local_file_path,
                                file_size=file_size, # Pass size for potential validation
                                profiling=prof # Pass profiling stats object
                            )
                            if profiling: profiling.add_timing('download_file_call', time.time_ns() - start_download)

                            if download_successful:
                                # Optional: Verify size after download
                                start_verify = time.time_ns() if profiling else 0
                                downloaded_size = local_file_path.stat().st_size
                                if downloaded_size == file_size:
                                    file_status = SyncState.SYNCED
                                    downloaded_this_file = True
                                else:
                                    logger.error(f"Downloaded file size mismatch for {local_file_path} (expected: {file_size}, got: {downloaded_size}). Download failed.")
                                    download_successful = False # Treat as failure
                                    error_message = "Downloaded size mismatch"
                                    file_status = SyncState.FAILED
                                    try:
                                        local_file_path.unlink() # Clean up corrupted download
                                    except OSError as e_unlink:
                                        logger.error(f"Failed to remove corrupted file {local_file_path}: {e_unlink}")
                                if profiling: profiling.add_timing('verify_download_size', time.time_ns() - start_verify)
                            else:
                                error_message = "Download function returned false"
                                file_status = SyncState.FAILED

                        except Exception as e:
                            logger.error(f"Error downloading {symbolic_remote_path} to {local_file_path}: {e}")
                            error_message = str(e)
                            file_status = SyncState.FAILED
                            # Attempt to clean up partially downloaded file
                            if local_file_path.exists():
                                try:
                                    local_file_path.unlink()
                                except OSError as e_unlink:
                                    logger.error(f"Failed to remove partial file {local_file_path}: {e_unlink}")

                    # --- State Update ---
                    file_hash = symbolic_path_to_hash.get(symbolic_remote_path)
                    op_status: OperationStatus
                    if file_status == SyncState.SYNCED or file_status == SyncState.SKIPPED:
                        op_status = "done"
                    elif file_status == SyncState.FAILED:
                        op_status = f"failed: {error_message or 'Unknown download error'}"
                    else: # Pending - should not happen here, but handle defensively
                         op_status = "pending"
                         
                    if state_file_path and file_hash is not None and file_status != SyncState.PENDING:
                        start_state_update = time.time_ns() if profiling else 0
                        update_operation_state_file(state_file_path, file_hash, op_status)
                        if profiling: profiling.add_timing('update_state_file', time.time_ns() - start_state_update)
                    # --- End State Update ---
                        
                    return file_status, file_size, error_message, downloaded_this_file

                # Process files in the current batch sequentially
                # (Parallelism happens across batches via ThreadPoolExecutor)
                for symbolic_path, size in batch_files:
                    status, f_size, err_msg, downloaded = _process_file(symbolic_path, size)
                    batch_results.append((symbolic_path, status, f_size, err_msg))
                    
                    # Update batch stats based on file status
                    if status == SyncState.SYNCED:
                        batch_stats.synced_files += 1
                        batch_stats.synced_size += f_size
                        if downloaded:
                             batch_stats.downloaded_files += 1
                             batch_stats.downloaded_size += f_size
                    elif status == SyncState.FAILED:
                        batch_stats.failed_files += 1
                    elif status == SyncState.SKIPPED:
                        batch_stats.skipped_files += 1
                        batch_stats.synced_files += 1 # Skipped files count as successfully synced overall
                        batch_stats.synced_size += f_size

                return batch_stats, batch_results


            # 4. Execute batches in parallel
            start_parallel = time.time_ns() if profiling else 0
            futures = []
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                for i, batch in enumerate(batches):
                    futures.append(executor.submit(process_batch, i, batch))

                # Process results as they complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        batch_stats, batch_results = future.result()
                        
                        # Update overall stats
                        stats.synced_files += batch_stats.synced_files
                        stats.synced_size += batch_stats.synced_size
                        stats.failed_files += batch_stats.failed_files
                        stats.skipped_files += batch_stats.skipped_files
                        stats.downloaded_files += batch_stats.downloaded_files
                        stats.downloaded_size += batch_stats.downloaded_size

                        # Update progress bars
                        files_processed_in_batch = len(batch_results)
                        bytes_processed_in_batch = sum(r[2] for r in batch_results) # Use actual file size reported
                        
                        processed_files_count += files_processed_in_batch
                        processed_bytes_count += bytes_processed_in_batch
                        
                        pbar_files.update(files_processed_in_batch)
                        pbar_bytes.update(bytes_processed_in_batch) # Update with actual byte size processed

                    except Exception as exc:
                        logger.error(f"Batch generated an exception: {exc}")
                        # How to account for failed batch in stats? Needs careful thought.
                        # Assume all files in the batch failed for now?
                        # This requires knowing the size of the failed batch.
                        # TODO: Improve error handling for batch failures.


            pbar_files.close()
            pbar_bytes.close()
            if profiling: profiling.add_timing('parallel_execution', time.time_ns() - start_parallel)


            # 5. Finalize and report
            if stats.failed_files > 0:
                logger.error(f"{stats.failed_files} files failed to sync. Check logs for details.")
                # --- Keep state file on failure ---
                if state_file_path:
                     logger.info(f"Sync finished with errors. Operation state file kept at: {state_file_path}")
                # --- End keep state file ---
            else:
                logger.info("Synchronization completed successfully.")
                # --- Delete state file on success ---
                if state_file_path:
                    start_state_delete = time.time_ns() if profiling else 0
                    delete_operation_state(state_file_path)
                    if profiling: profiling.add_timing('delete_state_file', time.time_ns() - start_state_delete)
                # --- End delete state file ---


            if enable_profiling and profiling:
                profiling.add_timing('total_sync_time', time.time_ns() - start_sync)
                logger.info("Profiling Summary (ms):")
                summary = profiling.get_summary()
                # Calculate total time from summary for percentage calculation
                total_ms = summary.get('total_sync_time', {}).get('total_ms', 1) # Avoid division by zero
                
                for op, data in sorted(summary.items(), key=lambda item: item[1]['total_ms'], reverse=True):
                     # Calculate percentage based on the total_sync_time operation
                     percentage = (data['total_ms'] / total_ms * 100) if total_ms > 0 else 0
                     logger.info(f"  - {op:<25}: "
                                f"Total: {data['total_ms']:.2f} ms, "
                                f"Avg: {data['avg_ms']:.3f} ms, "
                                f"Calls: {data['calls']}, "
                                f"Percentage: {percentage:.1f}%")

        except Exception as e:
            logger.exception(f"An unexpected error occurred during sync: {e}")
            stats.failed_files = stats.total_files - stats.synced_files - stats.skipped_files # Mark remaining as failed
            # Decide if state file should be deleted on unexpected error? Probably keep it.
            if state_file_path and state_file_path.exists():
                 logger.error(f"Sync failed unexpectedly. Operation state file kept at: {state_file_path}")
            # Re-raise the exception? Or just return stats? Depending on desired CLI behavior.
            # raise e 

        return stats

def clone_dataset(
    source_url: str,
    destination: Path,
    components: Optional[List[str]] = None,
    missing_component: Optional[str] = None,
    artists: Optional[List[str]] = None,
    proportion: Optional[float] = None,
    offset: int = 0,
    progress_callback = None,
    enable_profiling: bool = False,
    parallel: int = 1,
    use_http2: bool = False,
    connection_pool_size: int = 10,
    target_location: str = "Main"
):
    """Clone dataset from remote source.
    
    Args:
        source_url: Remote dataset URL
        destination: Local path for the cloned dataset
        components: List of components to clone
        missing_component: Only clone components for tracks missing this component
        artists: List of artists to clone
        proportion: Proportion of dataset to clone (0-1)
        offset: Offset for proportion-based cloning
        progress_callback: Callback function for progress updates
        enable_profiling: Enable performance profiling
        parallel: Number of parallel downloads (1 for sequential)
        use_http2: Whether to use HTTP/2 for connections
        connection_pool_size: Size of the connection pool
        target_location: Name of the target location
        
    Returns:
        Sync statistics
    """
    # Create destination directory
    destination.mkdir(parents=True, exist_ok=True)
    
    # Configure WebDAV client using the helper function
    client = configure_client(
        url=source_url,
        use_http2=use_http2,
        connection_pool_size=connection_pool_size
    )
    
    # Create blackbird directory
    blackbird_dir = destination / ".blackbird"
    blackbird_dir.mkdir(exist_ok=True)
    
    # Download schema
    schema_path = blackbird_dir / "schema.json"
    if not client.download_file(".blackbird/schema.json", schema_path):
        raise ValueError(f"Failed to download schema from {source_url}")
    
    # Load schema
    schema = DatasetComponentSchema.load(schema_path)
        
    # Download index
    index_path = blackbird_dir / "index.pickle"
    if not client.download_file(".blackbird/index.pickle", index_path):
        raise ValueError(f"Failed to download index from {source_url}")
            
    # Load index
    index = DatasetIndex.load(index_path)
        
    # Initialize sync manager
    # sync = DatasetSync(destination) # Old way
    # Need to create a Dataset object first
    dataset = Dataset(destination) 
    sync = DatasetSync(dataset) # Pass the dataset object
    
    # Perform sync
    stats = sync.sync(
        client=client,
        components=components or list(schema.schema["components"].keys()),
        artists=artists,
        missing_component=missing_component,
        resume=True,
        enable_profiling=enable_profiling,
        parallel=parallel,
        use_http2=use_http2,
        connection_pool_size=connection_pool_size,
        target_location_name=target_location
    )

    if progress_callback:
        progress_callback(stats, target_location)

    return stats

def configure_client(url: str, use_http2: bool = False, connection_pool_size: int = 10) -> WebDAVClient:
    """Configure WebDAV client from URL.
    
    Args:
        url: WebDAV URL (webdav://[user:pass@]host[:port]/path)
        use_http2: Whether to use HTTP/2 for connections
        connection_pool_size: Size of the connection pool
        
    Returns:
        Configured WebDAV client
    """
    return WebDAVClient(url, use_http2=use_http2, connection_pool_size=connection_pool_size) 

# New function for resuming sync
def resume_sync_operation(
    dataset_path: Path,
    state_file_path: Path,
    state: OperationState,
    enable_profiling: bool = False,
    parallel: int = 1,
    use_http2: bool = False,
    connection_pool_size: int = 10,
) -> bool:
    """Resumes a sync operation based on the provided state file."""
    
    logger.info(f"Starting resume for sync operation from {state_file_path}")
    
    try:
        # Initialize Dataset and load index/locations
        dataset = Dataset(dataset_path)
        # dataset.load_index() # Ensure index is loaded --> Incorrect method
        # Index is loaded automatically during Dataset init, access via dataset.index
        if not dataset.index: # Check if index loaded successfully
            logger.error("Failed to load or build dataset index. Cannot resume.")
            return False
            
        # Locations are loaded automatically by Dataset init
        # Ensure locations were loaded properly
        if not dataset.locations.get_all_locations():
            logger.error("Failed to load dataset locations. Cannot resume.")
            return False
            
        # Configure WebDAV client based on state
        client = configure_client(state['source'], use_http2=use_http2, connection_pool_size=connection_pool_size)
        
        # Filter files that need processing (pending or failed)
        files_to_process: Dict[int, Tuple[str, int]] = {}
        pending_count = 0
        failed_count = 0
        total_in_state = len(state['files'])
        
        logger.info("Analyzing state file to determine files to resume...")
        with tqdm(total=total_in_state, desc="Checking state", unit="file") as pbar:
            for file_hash, status in state['files'].items():
                pbar.update(1)
                if status == "pending" or status.startswith("failed"):
                    file_info = dataset.index.get_file_info_by_hash(file_hash)
                    if file_info:
                        symbolic_path, size = file_info
                        files_to_process[file_hash] = (symbolic_path, size)
                        if status == "pending":
                            pending_count += 1
                        else:
                            failed_count += 1
                    else:
                        logger.warning(f"Hash {file_hash} from state file not found in current index. Skipping.")
                # else: status is 'done', skip it

        logger.info(f"Found {len(files_to_process)} files to potentially resume ({pending_count} pending, {failed_count} failed).")
        
        if not files_to_process:
            logger.info("No files in 'pending' or 'failed' state. Nothing to resume.")
            delete_operation_state(state_file_path) # Clean up completed state file
            return True # Operation is effectively complete
        
        # Initialize stats
        stats = SyncStats(
            total_files=total_in_state, # Reflect total from the state file
            # Initialize counts based on what's already done
            synced_files=total_in_state - len(files_to_process), 
            failed_files=0, # Reset failed count for this resume attempt
            skipped_files=0
        )
        if enable_profiling:
            stats.enable_profiling()

        # Target location validation (already done in sync, but good to have here too)
        target_location_name = state['target_location']
        try:
            target_base_path = dataset.locations.get_location_path(target_location_name)
            if not target_base_path.is_dir(): # Ensure it exists and is a dir
                 raise FileNotFoundError(f"Target location '{target_location_name}' path does not exist or is not a directory: {target_base_path}")
        except (KeyError, FileNotFoundError) as e:
             logger.error(f"Invalid target location '{target_location_name}' specified in state file or configuration: {e}")
             return False # Cannot proceed without valid target

        # --- Resume Download Logic ---
        files_processed_in_resume = 0
        max_workers = parallel if parallel > 0 else 1
        processed_hashes_in_batch = set()
        batch_update_threshold = min(max(10, len(files_to_process) // 100), 500) # Update state file periodically

        with ThreadPoolExecutor(max_workers=max_workers) as executor, \
             tqdm(total=len(files_to_process), desc="Resuming download", unit="file") as pbar:
            
            futures = {}
            for file_hash, (symbolic_remote_path, expected_size) in files_to_process.items():
                future = executor.submit(
                    _process_file_for_resume, # Use a helper function
                    client=client,
                    dataset=dataset,
                    symbolic_remote_path=symbolic_remote_path,
                    expected_size=expected_size,
                    target_location_name=target_location_name,
                    file_hash=file_hash, # Pass hash for state update
                    profiling_stats=stats.profiling
                )
                futures[future] = file_hash

            for future in concurrent.futures.as_completed(futures):
                file_hash = futures[future]
                try:
                    symbolic_path, size, status, error_msg = future.result()
                    files_processed_in_resume += 1
                    
                    # Update overall stats
                    if status == SyncState.SYNCED:
                        stats.synced_files += 1
                        stats.downloaded_files += 1 # Assume resume means download
                        stats.synced_size += size
                        stats.downloaded_size += size
                        state_status = "done"
                    elif status == SyncState.SKIPPED:
                         stats.skipped_files += 1
                         state_status = "done" # Mark skipped as done in state
                    else: # FAILED
                        stats.failed_files += 1
                        state_status = f"failed: {error_msg}"
                    
                    # Update state file for this hash
                    update_operation_state_file(state_file_path, file_hash, state_status)
                    processed_hashes_in_batch.add(file_hash)

                    # Periodically log progress (optional)
                    # if len(processed_hashes_in_batch) >= batch_update_threshold:
                    #     logger.debug(f"Updated state file for {len(processed_hashes_in_batch)} files.")
                    #     processed_hashes_in_batch.clear()
                    
                except Exception as exc:
                    # Handle exceptions from the future itself (e.g., worker crash)
                    symbolic_path_unknown = files_to_process.get(file_hash, ("<unknown>", 0))[0]
                    logger.error(f"Error processing file {symbolic_path_unknown} (hash {file_hash}) during resume: {exc}", exc_info=True)
                    stats.failed_files += 1
                    # Update state file as failed
                    update_operation_state_file(state_file_path, file_hash, f"failed: {exc}")
                    processed_hashes_in_batch.add(file_hash)
                finally:
                    pbar.update(1)

        # Final check and cleanup
        if stats.failed_files == 0:
            logger.info("Resume completed successfully.")
            delete_operation_state(state_file_path) # Delete state file on success
            return True
        else:
            logger.warning(f"Resume finished with {stats.failed_files} errors. State file kept: {state_file_path}")
            return False
            
    except Exception as e:
        logger.error(f"Critical error during resume operation: {e}", exc_info=True)
        return False # Ensure state file is kept on unexpected errors

# Helper function for processing a single file during resume
def _process_file_for_resume(
    client: Any,
    dataset: Dataset,
    symbolic_remote_path: str,
    expected_size: int,
    target_location_name: str,
    file_hash: int, # Pass hash for potential logging/debugging
    profiling_stats: Optional[ProfilingStats] = None
) -> Tuple[str, int, SyncState, Optional[str]]:
    """Checks local file, downloads if needed, and returns status."""
    
    error_msg = None
    try:
        # 1. Resolve local path in the target location
        # Strip location prefix to get relative path for resolution
        parts = symbolic_remote_path.split('/', 1)
        if len(parts) != 2:
             raise ValueError(f"Invalid symbolic path format: {symbolic_remote_path}")
        relative_path_str = parts[1]
        
        # Construct symbolic path for the target location
        symbolic_local_path = f"{target_location_name}/{relative_path_str}"
        local_path = dataset.resolve_path(symbolic_local_path)

        # 2. Check if local file exists and matches size
        if local_path.exists():
            try:
                 local_size = local_path.stat().st_size
                 if local_size == expected_size:
                     # logger.debug(f"File already exists and matches size: {local_path}")
                     return symbolic_remote_path, expected_size, SyncState.SKIPPED, None
                 else:
                     logger.warning(f"File exists but size mismatch for {local_path}. Expected {expected_size}, got {local_size}. Re-downloading.")
                     # Attempt to remove the incorrect file before redownload
                     try:
                         # Add missing_ok=True
                         local_path.unlink(missing_ok=True)
                     except OSError as rm_err:
                         logger.error(f"Failed to remove existing file with incorrect size {local_path}: {rm_err}")
                         return symbolic_remote_path, 0, SyncState.FAILED, f"Failed to remove existing file: {rm_err}"
            except FileNotFoundError:
                # File vanished between exists() and stat(), proceed to download
                pass 
            except OSError as stat_err:
                logger.error(f"Error accessing existing file {local_path}: {stat_err}")
                return symbolic_remote_path, 0, SyncState.FAILED, f"Error accessing existing file: {stat_err}"

        # 3. Download if needed
        # Strip location prefix for remote download path
        remote_path_for_download = relative_path_str 
        
        # Use the internal _download_file method
        sync_instance = DatasetSync(dataset) # Create a temporary instance to access _download_file
        success, downloaded_size = sync_instance._download_file(
            client=client,
            remote_path=remote_path_for_download,
            local_path=local_path,
            file_size=expected_size,
            profiling=profiling_stats
        )
        
        if success:
            return symbolic_remote_path, downloaded_size, SyncState.SYNCED, None
        else:
            # _download_file should log the specific error
            # Attempt to clean up potentially partial file
            try:
                 if local_path.exists():
                     local_path.unlink()
            except OSError:
                 pass # Ignore cleanup error
            return symbolic_remote_path, 0, SyncState.FAILED, "Download failed"

    except SymbolicPathError as spe:
        error_msg = f"Symbolic path resolution error: {spe}"
        logger.error(f"Error processing {symbolic_remote_path} during resume: {error_msg}")
        return symbolic_remote_path, 0, SyncState.FAILED, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(f"Unexpected error processing {symbolic_remote_path} during resume: {error_msg}", exc_info=True)
        return symbolic_remote_path, 0, SyncState.FAILED, error_msg


# Keep configure_client at the end if it's a standalone utility function
def configure_client(url: str, use_http2: bool = False, connection_pool_size: int = 10) -> WebDAVClient:
    """Configure WebDAV client from URL."""
    return WebDAVClient(url, use_http2=use_http2, connection_pool_size=connection_pool_size) 