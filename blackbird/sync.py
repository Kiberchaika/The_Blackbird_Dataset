from pathlib import Path
from typing import List, Optional, Dict, Set, DefaultDict
import logging
from enum import Enum
from dataclasses import dataclass, field
from webdav3.client import Client
from tqdm import tqdm
import os
from urllib.parse import urlparse
import webdav3.client as webdav
import fnmatch
from difflib import get_close_matches
import click
from collections import defaultdict
import time

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
    
    def __init__(self, url: str):
        """Initialize WebDAV client.
        
        Args:
            url: WebDAV server URL with optional credentials
                Format: webdav://[user:pass@]host[:port]/path
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
            'webdav_timeout': 30,
            'disable_check': True  # Add this to prevent initial check
        }
        
        if username and password:
            options['webdav_login'] = username
            options['webdav_password'] = password
            
        self.client = webdav.Client(options)
        logger.info(f"WebDAV client configured for server: {self.base_url}")
        
    def check_connection(self) -> bool:
        """Check if the WebDAV server is accessible.
        
        Returns:
            bool: True if server is accessible
        """
        try:
            import requests
            response = requests.head(self.base_url)
            if response.status_code == 405:  # Method not allowed is OK, server is responding
                return True
            return 200 <= response.status_code < 400
        except Exception as e:
            logger.error(f"Failed to connect to WebDAV server: {e}")
            return False
            
    def list_files(self, path: str = '') -> List[Dict[str, str]]:
        """List files in remote directory.
        
        Args:
            path: Remote directory path
            
        Returns:
            List of file info dictionaries with 'path' and 'size' keys
        """
        try:
            if not self.check_connection():
                raise ValueError(f"Cannot connect to WebDAV server at {self.base_url}")
                
            logger.info(f"Listing files in path: {path}")
            files = self.client.list(path or '/', get_info=True)
            logger.info(f"Raw WebDAV response: {files[:5]}")  # Log first 5 entries
            
            result = [
                {
                    'path': f['path'].lstrip('/'),  # Remove leading slash
                    'size': int(f.get('size', 0))
                }
                for f in files
                if not f['path'].endswith('/')  # Skip directories
            ]
            logger.info(f"Processed {len(result)} files")
            return result
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {e}")
            raise
            
    def download_file(self, remote_path: str, local_path: Path, profiling: Optional[ProfilingStats] = None) -> bool:
        """Download a single file.
        
        Args:
            remote_path: Path on remote server
            local_path: Local path to save file
            profiling: Optional profiling stats
            
        Returns:
            True if download successful, False otherwise
        """
        start_total = time.time_ns() if profiling else 0
        
        try:
            # Ensure parent directory exists
            start_mkdir = time.time_ns() if profiling else 0
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if profiling:
                profiling.add_timing('mkdir', time.time_ns() - start_mkdir)
            
            # Try direct HTTP download first
            try:
                start_http = time.time_ns() if profiling else 0
                
                import requests
                url = f"{self.base_url}/{remote_path.lstrip('/')}"
                auth = None
                if hasattr(self.client.webdav, 'login') and hasattr(self.client.webdav, 'password'):
                    auth = (self.client.webdav.login, self.client.webdav.password)
                
                if profiling:
                    start_http_setup = time.time_ns()
                    profiling.add_timing('http_setup', start_http_setup - start_http)
                
                start_http_request = time.time_ns() if profiling else 0
                response = requests.get(url, auth=auth)
                if profiling:
                    profiling.add_timing('http_request', time.time_ns() - start_http_request)
                
                if response.status_code == 200:
                    start_write = time.time_ns() if profiling else 0
                    local_path.write_bytes(response.content)
                    if profiling:
                        profiling.add_timing('file_write', time.time_ns() - start_write)
                    if profiling:
                        profiling.add_timing('http_download_total', time.time_ns() - start_http)
                    return True
                elif response.status_code == 404:
                    logger.error(f"File not found: {remote_path}")
                    return False
                else:
                    logger.error(f"Failed to download {remote_path}: HTTP {response.status_code}")
                    return False
                    
            except Exception as e:
                # Fall back to WebDAV client if direct HTTP fails
                logger.warning(f"Direct HTTP download failed, trying WebDAV: {e}")
                try:
                    start_webdav = time.time_ns() if profiling else 0
                    self.client.download_sync(
                        remote_path=remote_path,
                        local_path=str(local_path)
                    )
                    if profiling:
                        profiling.add_timing('webdav_download', time.time_ns() - start_webdav)
                    return True
                except Exception as e2:
                    logger.error(f"WebDAV download also failed: {e2}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to download {remote_path}: {e}")
            return False
        finally:
            if profiling:
                profiling.add_timing('download_total', time.time_ns() - start_total)

class DatasetSync:
    """Manages dataset synchronization using WebDAV."""
    
    def __init__(self, local_path: Path):
        """Initialize sync manager.
        
        Args:
            local_path: Local path to sync to
        """
        self.local_path = Path(local_path)
        self.schema_path = self.local_path / ".blackbird" / "schema.json"
        self.index_path = self.local_path / ".blackbird" / "index.pickle"
        
        # Load schema and index if they exist
        if self.schema_path.exists():
            self.schema = DatasetComponentSchema.load(self.schema_path)
        else:
            raise ValueError(f"Schema not found at {self.schema_path}")
            
        if self.index_path.exists():
            self.index = DatasetIndex.load(self.index_path)
        else:
            raise ValueError(f"Index not found at {self.index_path}")
    
    def configure_client(self, webdav_url: str, username: str, password: str) -> Client:
        """Configure WebDAV client.
        
        Args:
            webdav_url: WebDAV server URL
            username: WebDAV username
            password: WebDAV password
            
        Returns:
            Configured WebDAV client
        """
        options = {
            'webdav_hostname': webdav_url,
            'webdav_login': username,
            'webdav_password': password,
            'disable_check': True
        }
        return Client(options)
    
    def sync(
        self,
        client: Client,
        components: List[str],
        artists: Optional[List[str]] = None,
        albums: Optional[List[str]] = None,
        missing_component: Optional[str] = None,
        resume: bool = True,
        enable_profiling: bool = False
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
            
        Returns:
            Sync statistics
        """
        start_total = time.time_ns() if enable_profiling else 0
        
        stats = SyncStats()
        if enable_profiling:
            stats.enable_profiling()
        
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
        
        logger.info(f"Found {stats.total_files} files to sync ({stats.total_size / (1024*1024*1024):.2f} GB)")
        
        # Second pass: sync files
        with tqdm(total=stats.total_files, desc="Syncing files") as pbar:
            for file_path, file_size in files_to_sync.items():
                start_file_sync = time.time_ns() if enable_profiling else 0
                
                local_file = self.local_path / file_path
                
                # Check if file exists
                start_check_exists = time.time_ns() if enable_profiling and stats.profiling else 0
                file_exists = resume and local_file.exists() and local_file.stat().st_size == file_size
                if enable_profiling and stats.profiling:
                    stats.profiling.add_timing('check_exists', time.time_ns() - start_check_exists)
                
                # Skip if file exists and resume is True
                if file_exists:
                    logger.debug(f"Skipping existing file: {file_path}")
                    stats.skipped_files += 1
                    pbar.update(1)
                    continue
                
                try:
                    # Download file
                    start_download = time.time_ns() if enable_profiling else 0
                    
                    # Use WebDAVClient.download_file if available, otherwise direct download_sync
                    if hasattr(client, 'download_file'):
                        success = client.download_file(
                            remote_path=file_path,
                            local_path=local_file,
                            profiling=stats.profiling if enable_profiling else None
                        )
                    else:
                        # Create parent directory if it doesn't exist
                        local_file.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Use client.download_sync directly
                        client.download_sync(
                            remote_path=file_path,
                            local_path=str(local_file)
                        )
                        success = True
                    
                    if enable_profiling and stats.profiling:
                        stats.profiling.add_timing('download', time.time_ns() - start_download)
                    
                    if success:
                        stats.synced_files += 1
                        stats.synced_size += file_size
                        stats.downloaded_files += 1
                        stats.downloaded_size += file_size
                    else:
                        stats.failed_files += 1
                    
                except Exception as e:
                    logger.error(f"Failed to sync {file_path}: {e}")
                    stats.failed_files += 1
                
                pbar.update(1)
                
                if enable_profiling and stats.profiling:
                    stats.profiling.add_timing('file_sync_total', time.time_ns() - start_file_sync)
        
        if enable_profiling and stats.profiling:
            stats.profiling.add_timing('sync_total', time.time_ns() - start_total)
        
        logger.info(f"Synced {stats.synced_files} files, failed {stats.failed_files}, skipped {stats.skipped_files}")
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
    enable_profiling: bool = False
) -> SyncStats:
    """Clone a remote dataset to local storage.
    
    Args:
        source_url: WebDAV URL of the source dataset
        destination: Local destination path
        components: Optional list of components to clone (default: all)
        missing_component: Optional component that must be missing in the track
        artists: Optional list of artists to filter by
        proportion: Optional proportion of the dataset to clone (0-1)
        offset: Optional offset for proportion-based cloning
        progress_callback: Optional callback for progress updates
        enable_profiling: Enable performance profiling
        
    Returns:
        Sync statistics
    """
    start_total = time.time_ns() if enable_profiling else 0
    
    stats = SyncStats()
    if enable_profiling:
        stats.enable_profiling()
    
    # Create destination directory if it doesn't exist
    start_setup = time.time_ns() if enable_profiling else 0
    destination.mkdir(parents=True, exist_ok=True)
    
    # Create blackbird directory
    blackbird_dir = destination / ".blackbird"
    blackbird_dir.mkdir(exist_ok=True)
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('setup_dirs', time.time_ns() - start_setup)
    
    # Configure WebDAV client
    start_connect = time.time_ns() if enable_profiling else 0
    client = configure_client(source_url)
    
    # Check connection
    if not client.check_connection():
        raise ConnectionError(f"Could not connect to WebDAV server at {source_url}")
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('connect', time.time_ns() - start_connect)
    
    # Download schema and index files
    start_meta = time.time_ns() if enable_profiling else 0
    
    logger.info("Downloading schema and index files...")
    
    # Download schema
    schema_local = blackbird_dir / "schema.json"
    schema_remote = ".blackbird/schema.json"
    
    if not client.download_file(schema_remote, schema_local):
        raise FileNotFoundError(f"Could not download schema file from {source_url}")
    
    # Download index
    index_local = blackbird_dir / "index.pickle"
    index_remote = ".blackbird/index.pickle"
    
    if not client.download_file(index_remote, index_local):
        raise FileNotFoundError(f"Could not download index file from {source_url}")
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('download_meta', time.time_ns() - start_meta)
    
    # Load schema and index
    start_load = time.time_ns() if enable_profiling else 0
    local_schema = DatasetComponentSchema.load(schema_local.parent)
    remote_index = DatasetIndex.load(index_local)
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('load_meta', time.time_ns() - start_load)
    
    # Filter tracks by criteria
    start_filter = time.time_ns() if enable_profiling else 0
    
    # Build component pattern map for output
    component_patterns = {name: info["pattern"] for name, info in local_schema.schema["components"].items()}
    component_counts = defaultdict(int)
    
    # Get track list
    all_tracks = remote_index.tracks
    
    # Calculate total tracks by component
    for track in all_tracks.values():
        for comp_name in track.files.keys():
            component_counts[comp_name] += 1
    
    # Filter by criteria
    tracks_to_process = {}
    
    # Apply artist filter if specified
    if artists:
        for artist_pattern in artists:
            for artist in remote_index.album_by_artist.keys():
                if fnmatch.fnmatch(artist.lower(), artist_pattern.lower()):
                    for album in remote_index.album_by_artist[artist]:
                        for track in remote_index.track_by_album[album]:
                            tracks_to_process[track] = remote_index.tracks[track]
    else:
        tracks_to_process = all_tracks.copy()
    
    # Filter by missing component if specified
    if missing_component:
        if missing_component not in local_schema.schema["components"]:
            raise ValueError(f"Invalid component: {missing_component}")
            
        tracks_to_process = {k: v for k, v in tracks_to_process.items() if missing_component not in v.files}
    
    # Apply proportion filter if specified
    if proportion is not None:
        if proportion <= 0 or proportion > 1:
            raise ValueError("Proportion must be between 0 and 1")
            
        # Get sorted list of tracks
        track_list = sorted(tracks_to_process.keys())
        
        # Calculate slice
        total_tracks = len(track_list)
        slice_size = int(total_tracks * proportion)
        end_idx = min(offset + slice_size, total_tracks)
        
        # Apply slice
        track_slice = track_list[offset:end_idx]
        tracks_to_process = {k: tracks_to_process[k] for k in track_slice}
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('filter_tracks', time.time_ns() - start_filter)
    
    logger.info(f"Processing {len(tracks_to_process)} tracks")
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('filter_tracks', time.time_ns() - start_filter)
        
    # Check if requested components have files
    if components:
        missing_files = [comp for comp in components if component_counts[comp] == 0]
        if missing_files:
            logger.warning(f"\nWarning: The following components have no files in the index:")
            for comp in missing_files:
                pattern = component_patterns.get(comp, "unknown pattern")
                logger.warning(f"- {comp} (pattern: {pattern})")
            if not click.confirm("\nContinue anyway?", default=False):
                raise ValueError("Aborted due to missing files for requested components")
    
    # Find all files to download based on index
    start_prepare = time.time_ns() if enable_profiling else 0
    all_files = []  # List of (file_path, file_size) tuples
    stats.total_files = 0
    stats.total_size = 0
    
    # Process filtered tracks
    for track_info in tracks_to_process.values():
        # Check each requested component
        target_components = components if components else local_schema.schema['components'].keys()
        for component in target_components:
            if component in track_info.files:
                file_path = track_info.files[component]
                file_size = track_info.file_sizes[file_path]
                all_files.append((file_path, file_size))
                stats.total_files += 1
                stats.total_size += file_size
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('prepare_download', time.time_ns() - start_prepare)
    
    # Download files with progress bar
    with tqdm(total=stats.total_size, unit='B', unit_scale=True) as pbar:
        for file_path, file_size in all_files:
            start_file_download = time.time_ns() if enable_profiling else 0
            
            local_path = destination / file_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Skip if file exists with correct size
            start_check = time.time_ns() if enable_profiling and stats.profiling else 0
            skip_file = local_path.exists() and local_path.stat().st_size == file_size
            if enable_profiling and stats.profiling:
                stats.profiling.add_timing('check_exists', time.time_ns() - start_check)
            
            if skip_file:
                stats.skipped_files += 1
                pbar.update(file_size)
                continue
            
            # Download file
            start_download = time.time_ns() if enable_profiling else 0
            if client.download_file(file_path, local_path, stats.profiling if enable_profiling else None):
                stats.downloaded_files += 1
                stats.downloaded_size += file_size
                pbar.update(file_size)
            else:
                stats.failed_files += 1
            
            if enable_profiling and stats.profiling:
                stats.profiling.add_timing('file_download', time.time_ns() - start_file_download)
    
    if enable_profiling and stats.profiling:
        stats.profiling.add_timing('clone_total', time.time_ns() - start_total)
        
        # Print profiling stats if enabled
        print("\nProfiling Statistics:")
        profile_summary = stats.profiling.get_summary()
        
        # Sort operations by percentage of total time
        sorted_ops = sorted(profile_summary.items(), key=lambda x: x[1]['percentage'], reverse=True)
        
        for op, metrics in sorted_ops:
            print(f"  {op}:")
            print(f"    Total: {metrics['total_ms']:.2f} ms")
            print(f"    Calls: {metrics['calls']}")
            print(f"    Avg: {metrics['avg_ms']:.2f} ms per call")
            print(f"    Percentage: {metrics['percentage']:.2f}%")
    
    return stats

def configure_client(url: str) -> 'WebDAVClient':
    """Configure WebDAV client for remote dataset access.
    
    Args:
        url: WebDAV server URL (e.g. http://localhost:8080)
        
    Returns:
        Configured WebDAV client
    """
    return WebDAVClient(url) 