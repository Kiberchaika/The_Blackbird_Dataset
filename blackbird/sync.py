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
    
    def download_file(self, remote_path: str, local_path: Path, profiling: Optional[ProfilingStats] = None) -> bool:
        """Download a file from the WebDAV server.
        
        Args:
            remote_path: Path to the file on the server
            local_path: Local path to save the file
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
    """Sync manager for dataset synchronization."""
    
    def __init__(self, local_path: Path):
        """Initialize sync manager.
        
        Args:
            local_path: Path to local dataset
        """
        self.local_path = Path(local_path)
        self.blackbird_dir = self.local_path / ".blackbird"
        self.schema_path = self.blackbird_dir / "schema.json"
        self.index_path = self.blackbird_dir / "index.pickle"
        
        if self.schema_path.exists():
            self.schema = DatasetComponentSchema.load(self.schema_path)
        else:
            raise ValueError(f"Schema not found at {self.schema_path}")
            
        if self.index_path.exists():
            self.index = DatasetIndex.load(self.index_path)
        else:
            raise ValueError(f"Index not found at {self.index_path}")
    
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
    
    def _download_file(self, client: Any, file_path: str, local_file: Path, 
                      file_size: int, profiling: Optional[ProfilingStats] = None) -> Tuple[bool, int]:
        """Download a single file.
        
        Args:
            client: WebDAV client
            file_path: Path to the file on the server
            local_file: Local path to save the file
            file_size: Expected file size
            profiling: Optional profiling stats object
            
        Returns:
            Tuple of (success, file_size)
        """
        start_file_sync = time.time_ns() if profiling else 0
        
        # Check if file exists
        start_check_exists = time.time_ns() if profiling else 0
        file_exists = local_file.exists() and local_file.stat().st_size == file_size
        if profiling:
            profiling.add_timing('check_exists', time.time_ns() - start_check_exists)
        
        # Skip if file exists
        if file_exists:
            logger.debug(f"Skipping existing file: {file_path}")
            if profiling:
                profiling.add_timing('file_sync_total', time.time_ns() - start_file_sync)
            return (False, file_size)  # Skipped
        
        try:
            # Download file
            start_download = time.time_ns() if profiling else 0
            
            # Use WebDAVClient.download_file
            success = client.download_file(
                remote_path=file_path,
                local_path=local_file,
                profiling=profiling
            )
            
            if profiling:
                profiling.add_timing('download', time.time_ns() - start_download)
            
            if profiling:
                profiling.add_timing('file_sync_total', time.time_ns() - start_file_sync)
            
            return (success, file_size)
            
        except Exception as e:
            logger.error(f"Failed to sync {file_path}: {e}")
            if profiling:
                profiling.add_timing('file_sync_total', time.time_ns() - start_file_sync)
            return (False, 0)  # Failed
    
    def sync(
        self,
        client: Any,
        components: List[str],
        artists: Optional[List[str]] = None,
        albums: Optional[List[str]] = None,
        missing_component: Optional[str] = None,
        resume: bool = True,
        enable_profiling: bool = False,
        parallel: int = 1,
        use_http2: bool = False,
        connection_pool_size: int = 10,
        target_location_name: str = "Main"
    ) -> SyncStats:
        """Sync dataset from WebDAV server.
        
        Args:
            client: WebDAV client
            components: List of components to sync
            artists: Optional list of artists to sync
            albums: Optional list of albums to sync (requires artists to be specified)
            missing_component: Optional component that must be missing in the track
            resume: Whether to resume existing downloads
            enable_profiling: Enable performance profiling
            parallel: Number of parallel downloads (1 for sequential)
            use_http2: Whether to use HTTP/2 for connections
            connection_pool_size: Size of the connection pool
            target_location_name: Name of the target location
            
        Returns:
            Sync statistics
        """
        # Force enable color for tqdm
        os.environ['FORCE_COLOR'] = '1'
        
        start_total_time = time.time()
        start_total = time.time_ns() if enable_profiling else 0
        
        stats = SyncStats()
        if enable_profiling:
            stats.enable_profiling()
        
        # Ensure target location exists
        if target_location_name not in self.dataset.locations.get_all_locations():
            raise ValueError(f"Target location '{target_location_name}' not found in locations.json")
        
        # Validate components
        invalid_components = [c for c in components if c not in self.schema.schema["components"]]
        if invalid_components:
            raise ValueError(f"Invalid components: {', '.join(invalid_components)}")
        
        # Filter tracks to sync
        if artists:
            # Use glob pattern matching for artist filtering
            artist_patterns = artists
            matched_artists = set()
            for pattern in artist_patterns:
                for artist in self.index.album_by_artist.keys():
                    if fnmatch.fnmatch(artist.lower(), pattern.lower()):
                        matched_artists.add(artist)
            
            if not matched_artists:
                close_matches = []
                for pattern in artist_patterns:
                    close = get_close_matches(pattern, self.index.album_by_artist.keys(), n=5, cutoff=0.6)
                    close_matches.extend(close)
                
                if close_matches:
                    logger.info(f"No artists matched your patterns. Did you mean one of these?")
                    for artist in sorted(set(close_matches))[:10]:
                        logger.info(f"- {artist}")
                raise ValueError(f"No artists found matching patterns: {artists}")
            
            logger.info(f"Filtered to {len(matched_artists)} artists")
        else:
            matched_artists = set(self.index.album_by_artist.keys())
        
        start_find_files = time.time_ns() if enable_profiling else 0
        
        # Find all files to sync
        files_to_sync = {}  # path -> size
        
        # First pass: determine files to sync
        for artist in matched_artists:
            for album_path in self.index.album_by_artist[artist]:
                # Skip if albums are specified and this album is not in the list
                if albums:
                    album_name = Path(album_path).name
                    if album_name not in albums:
                        continue
                        
                for track_path in self.index.track_by_album.get(album_path, []):
                    track = self.index.tracks.get(track_path)
                    if track:
                        # Skip if we're looking for tracks missing a component and this track has it
                        if missing_component and missing_component in track.files:
                            continue
                            
                    for component in components:
                        if component in track.files:
                            file_path = track.files[component]
                            file_size = track.file_sizes[file_path]
                            files_to_sync[file_path] = file_size
                            stats.total_files += 1
                            stats.total_size += file_size
        
        if enable_profiling and stats.profiling:
            stats.profiling.add_timing('find_files', time.time_ns() - start_find_files)
        
        # Display download information before starting
        logger.info(f"{Fore.CYAN}Found {Fore.YELLOW}{stats.total_files}{Fore.CYAN} files to sync ({Fore.YELLOW}{stats.total_size / (1024*1024*1024):.2f}{Fore.CYAN} GB)")
        
        # Check if files exist already (to estimate actual download size)
        existing_files = 0
        existing_size = 0
        for file_path, file_size in files_to_sync.items():
            local_file = self.local_path / file_path
            if resume and local_file.exists():
                # Check if file size matches expected size
                try:
                    actual_size = local_file.stat().st_size
                    if actual_size == file_size:
                        existing_files += 1
                        existing_size += file_size
                        logger.debug(f"Found existing file: {local_file} (size: {actual_size} bytes)")
                    else:
                        logger.debug(f"File exists but size mismatch: {local_file} (expected: {file_size}, actual: {actual_size})")
                except Exception as e:
                    logger.debug(f"Error checking file size for {local_file}: {e}")
            elif resume and not local_file.exists():
                logger.debug(f"File does not exist: {local_file}")
        
        # Log summary of existing files
        if existing_files > 0:
            logger.info(f"Found {existing_files} existing files ({existing_size / (1024*1024*1024):.2f} GB) that can be skipped")
        
        files_to_download = stats.total_files - existing_files
        size_to_download = stats.total_size - existing_size
        
        # Display download information with colors
        print(f"\n{Fore.CYAN}{Style.BRIGHT}Download Information:{Style.RESET_ALL}")
        print(f"  {Fore.WHITE}Total files: {Fore.YELLOW}{stats.total_files}")
        print(f"  {Fore.WHITE}Files to download: {Fore.GREEN}{files_to_download}")
        print(f"  {Fore.WHITE}Files already existing: {Fore.BLUE}{existing_files}")
        print(f"  {Fore.WHITE}Total size: {Fore.YELLOW}{stats.total_size / (1024*1024*1024):.2f} GB")
        print(f"  {Fore.WHITE}Size to download: {Fore.GREEN}{size_to_download / (1024*1024*1024):.2f} GB")
        if parallel > 1:
            print(f"  {Fore.WHITE}Using {Fore.MAGENTA}{parallel} parallel download threads")
        if use_http2:
            print(f"  {Fore.WHITE}Using {Fore.MAGENTA}HTTP/2 protocol")
        if connection_pool_size > 0:
            print(f"  {Fore.WHITE}Using connection pool size: {Fore.MAGENTA}{connection_pool_size}")
        print("")
        
        # Track errors to summarize later
        error_counts = defaultdict(int)
        
        # Second pass: sync files
        if parallel > 1:
            # Parallel download using ThreadPoolExecutor
            # Split files into batches for each worker
            file_items = list(files_to_sync.items())
            batch_size = len(file_items) // parallel
            batches = []
            
            for i in range(parallel):
                start_idx = i * batch_size
                end_idx = start_idx + batch_size if i < parallel - 1 else len(file_items)
                batches.append(file_items[start_idx:end_idx])
            
            # Create a lock for updating shared stats
            from threading import Lock
            stats_lock = Lock()
            
            # Function to process a batch with its own progress bar
            def process_batch(batch_id, batch_files):
                batch_stats = {
                    "downloaded": 0,
                    "skipped": 0,
                    "failed": 0,
                    "downloaded_size": 0,
                    "start_time": time.time()
                }
                batch_errors = defaultdict(int)
                
                # Calculate total batch size in MB
                total_batch_size_mb = sum(size for _, size in batch_files) / (1024*1024)
                
                # Define color based on batch_id
                colors = ['green', 'blue', 'magenta', 'cyan', 'yellow', 'red']
                color = colors[batch_id % len(colors)]
                
                with tqdm(
                    total=len(batch_files), 
                    desc=f"Thread {batch_id+1}/{parallel}", 
                    position=batch_id,
                    leave=True,
                    unit="file",
                    colour=color,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
                ) as pbar:
                    for symbolic_file_path, file_size in batch_files:
                        try:
                            # Extract relative path and resolve local path
                            parts = symbolic_file_path.split('/', 1)
                            if len(parts) != 2:
                                raise ValueError(f"Invalid symbolic path format: {symbolic_file_path}")
                            _original_loc, relative_path = parts
                            target_location_path = self.dataset.locations.get_location_path(target_location_name)
                            local_file = target_location_path / relative_path
                            remote_path_to_download = relative_path
                        except Exception as e:
                            logger.error(f"Error processing path {symbolic_file_path}: {e}")
                            with stats_lock: stats.failed_files += 1
                            pbar.update(1)
                            continue

                        # Check existence in target location
                        if resume and local_file.exists():
                            try:
                                actual_size = local_file.stat().st_size
                                if actual_size == file_size:
                                    with stats_lock:
                                        stats.skipped_files += 1
                                    batch_stats["skipped"] += 1
                                    pbar.update(1)
                                    continue
                            except Exception as e:
                                logger.warning(f"Could not check existing file {local_file}: {e}")

                        # Download file (pass relative_path)
                        success, downloaded_size = self._download_file(client, remote_path_to_download, local_file, file_size, stats.profiling if enable_profiling else None)

                        # Update stats based on download result
                        with stats_lock:
                            if success:
                                stats.downloaded_files += 1
                                stats.downloaded_size += downloaded_size
                                stats.synced_files += 1 # Count successful downloads as synced
                                stats.synced_size += downloaded_size # Use actual downloaded size
                                batch_stats["downloaded"] += 1
                                batch_stats["downloaded_size"] += downloaded_size
                            else:
                                stats.failed_files += 1
                                error_message = "Download Failed"
                                batch_errors[error_message] += 1

                        pbar.update(1)
                        # Update progress bar postfix with batch stats
                        pbar.set_postfix({
                            'down': batch_stats['downloaded'],
                            'skip': batch_stats['skipped'],
                            'fail': batch_stats['failed'],
                            'size_MB': f"{batch_stats['downloaded_size'] / (1024*1024):.1f}"
                        })
                
                # Log batch errors
                with stats_lock:
                    for error, count in batch_errors.items():
                        error_counts[error] += count
                
                return batch_stats
            
            # Create and start threads for each batch
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                futures = []
                for i, batch in enumerate(batches):
                    futures.append(executor.submit(process_batch, i, batch))
                
                # Wait for all batches to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Batch processing error: {e}")
        else:
            # Sequential download
            with tqdm(
                total=stats.total_files,
                desc="Syncing files", 
                unit="file",
                colour="blue", # Or another color
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
            ) as pbar:
                for symbolic_file_path, file_size in files_to_sync.items():
                    try:
                        # Extract relative path and resolve local path
                        parts = symbolic_file_path.split('/', 1)
                        if len(parts) != 2:
                            raise ValueError(f"Invalid symbolic path format: {symbolic_file_path}")
                        _original_loc, relative_path = parts
                        target_location_path = self.dataset.locations.get_location_path(target_location_name)
                        local_file = target_location_path / relative_path
                        remote_path_to_download = relative_path
                    except Exception as e:
                        logger.error(f"Error processing path {symbolic_file_path}: {e}")
                        stats.failed_files += 1
                        pbar.update(1)
                        continue
                    
                    # Check existence in target location
                    if resume and local_file.exists():
                        try:
                            actual_size = local_file.stat().st_size
                            if actual_size == file_size:
                                stats.skipped_files += 1
                                pbar.update(1)
                                pbar.set_postfix({
                                    'skip': stats.skipped_files,
                                    'fail': stats.failed_files,
                                    'size_GB': f"{stats.downloaded_size / (1024*1024*1024):.2f}"
                                })
                                continue # Skip download
                        except Exception as e:
                            logger.warning(f"Could not check existing file {local_file}: {e}")

                    # Download file (pass relative_path)
                    success, downloaded_size = self._download_file(client, remote_path_to_download, local_file, file_size, stats.profiling if enable_profiling else None)
                    
                    # Update stats based on download result
                    if success:
                        stats.downloaded_files += 1
                        stats.downloaded_size += downloaded_size
                        stats.synced_files += 1 # Count successful downloads as synced
                        stats.synced_size += downloaded_size # Use actual downloaded size
                    else:
                        stats.failed_files += 1
                        error_message = "Download Failed"
                        error_counts[error_message] += 1
                        
                    pbar.update(1)
                    # Update progress bar postfix
                    pbar.set_postfix({
                        'down': stats.downloaded_files,
                        'skip': stats.skipped_files,
                        'fail': stats.failed_files,
                        'size_GB': f"{stats.downloaded_size / (1024*1024*1024):.2f}"
                    })

        if enable_profiling and stats.profiling:
            stats.profiling.add_timing('sync_total', time.time_ns() - start_total)
        
        # Calculate total elapsed time
        total_elapsed_time = time.time() - start_total_time
        hours, remainder = divmod(total_elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Calculate download speed
        download_speed_mbps = (stats.downloaded_size / (1024*1024)) / total_elapsed_time if total_elapsed_time > 0 else 0
        
        # Print detailed summary with colors
        print(f"\n{Fore.CYAN}{Style.BRIGHT}" + "="*50)
        print(f"{Fore.CYAN}{Style.BRIGHT}DOWNLOAD SUMMARY")
        print(f"{Fore.CYAN}{Style.BRIGHT}" + "="*50)
        print(f"{Fore.WHITE}Total time: {Fore.YELLOW}{int(hours):02d}:{int(minutes):02d}:{seconds:.2f}")
        print(f"{Fore.WHITE}Files processed: {Fore.YELLOW}{stats.total_files}")
        print(f"{Fore.WHITE}Files downloaded: {Fore.GREEN}{stats.downloaded_files}")
        print(f"{Fore.WHITE}Files skipped: {Fore.BLUE}{stats.skipped_files}")
        print(f"{Fore.WHITE}Files failed: {Fore.RED}{stats.failed_files}")
        print(f"{Fore.WHITE}Total size: {Fore.YELLOW}{stats.total_size / (1024*1024*1024):.2f} GB")
        print(f"{Fore.WHITE}Downloaded size: {Fore.GREEN}{stats.downloaded_size / (1024*1024*1024):.2f} GB")
        print(f"{Fore.WHITE}Average download speed: {Fore.MAGENTA}{download_speed_mbps:.2f} MB/s")
        if parallel > 1:
            print(f"{Fore.WHITE}Parallel threads used: {Fore.MAGENTA}{parallel}")
        print(f"{Fore.CYAN}{Style.BRIGHT}" + "="*50)
        
        # Print error summary
        if error_counts:
            logger.error("\nError Summary:")
            for error, count in error_counts.items():
                logger.error(f"- {error}: {count} times")
        
        logger.info(f"{Fore.GREEN}Synced {stats.synced_files} files, {Fore.RED}failed {stats.failed_files}, {Fore.BLUE}skipped {stats.skipped_files}")
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
    
    # Configure WebDAV client
    client = WebDAVClient(
        source_url,
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
    sync = DatasetSync(destination)
    
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