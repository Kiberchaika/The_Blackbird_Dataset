from pathlib import Path
from typing import List, Optional, Dict, Set
import logging
from enum import Enum
from dataclasses import dataclass
from webdav3.client import Client
from tqdm import tqdm
import os
from urllib.parse import urlparse
import webdav3.client as webdav
import fnmatch
from difflib import get_close_matches
import click
from collections import defaultdict

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
            
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a single file.
        
        Args:
            remote_path: Path on remote server
            local_path: Local path to save file
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Try direct HTTP download first
            try:
                import requests
                url = f"{self.base_url}/{remote_path.lstrip('/')}"
                auth = None
                if hasattr(self.client.webdav, 'login') and hasattr(self.client.webdav, 'password'):
                    auth = (self.client.webdav.login, self.client.webdav.password)
                
                response = requests.get(url, auth=auth)
                if response.status_code == 200:
                    local_path.write_bytes(response.content)
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
                    self.client.download_sync(
                        remote_path=remote_path,
                        local_path=str(local_path)
                    )
                    return True
                except Exception as e2:
                    logger.error(f"WebDAV download also failed: {e2}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to download {remote_path}: {e}")
            return False

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
        resume: bool = True
    ) -> SyncStats:
        """Sync specified components for selected artists.
        
        Args:
            client: WebDAV client
            components: List of components to sync
            artists: Optional list of artists to sync (all if None)
            resume: Whether to resume previous sync
            
        Returns:
            Sync statistics
        """
        # Validate components
        invalid_components = set(components) - set(self.schema.schema["components"].keys())
        if invalid_components:
            raise ValueError(f"Invalid components: {invalid_components}")
        
        # Get list of artists to sync
        artists_to_sync = artists if artists else list(self.index.album_by_artist.keys())
        
        # Initialize stats
        stats = SyncStats()
        
        # First pass: collect all files to sync and calculate total size
        logger.info("Collecting files to sync...")
        files_to_sync: Dict[str, int] = {}  # file_path -> size
        
        for artist in artists_to_sync:
            # Get all tracks for this artist
            for album_path in self.index.album_by_artist[artist]:
                for track_path in self.index.track_by_album[album_path]:
                    track = self.index.tracks[track_path]
                    
                    # Check each requested component
                    for component in components:
                        if component in track.files:
                            file_path = track.files[component]
                            file_size = track.file_sizes[file_path]
                            files_to_sync[file_path] = file_size
                            stats.total_files += 1
                            stats.total_size += file_size
        
        logger.info(f"Found {stats.total_files} files to sync ({stats.total_size / (1024*1024*1024):.2f} GB)")
        
        # Second pass: sync files
        with tqdm(total=stats.total_files, desc="Syncing files") as pbar:
            for file_path, file_size in files_to_sync.items():
                local_file = self.local_path / file_path
                
                # Create parent directory if it doesn't exist
                local_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Skip if file exists and resume is True
                if resume and local_file.exists() and local_file.stat().st_size == file_size:
                    logger.debug(f"Skipping existing file: {file_path}")
                    stats.skipped_files += 1
                    pbar.update(1)
                    continue
                
                try:
                    # Download file
                    client.download_sync(
                        remote_path=file_path,
                        local_path=str(local_file)
                    )
                    
                    # Verify file size
                    if local_file.stat().st_size == file_size:
                        stats.synced_files += 1
                        stats.synced_size += file_size
                        logger.debug(f"Successfully synced: {file_path}")
                    else:
                        stats.failed_files += 1
                        logger.error(f"Size mismatch for {file_path}")
                        try:
                            local_file.unlink()
                        except:
                            pass
                except Exception as e:
                    stats.failed_files += 1
                    logger.error(f"Failed to sync {file_path}: {str(e)}")
                    try:
                        local_file.unlink()
                    except:
                        pass
                
                pbar.update(1)
        
        # Log final statistics
        logger.info("\nSync completed!")
        logger.info(f"Total files: {stats.total_files}")
        logger.info(f"Successfully synced: {stats.synced_files}")
        logger.info(f"Failed: {stats.failed_files}")
        logger.info(f"Skipped: {stats.skipped_files}")
        logger.info(f"Total size: {stats.total_size / (1024*1024*1024):.2f} GB")
        logger.info(f"Synced size: {stats.synced_size / (1024*1024*1024):.2f} GB")
        
        return stats 

def clone_dataset(
    source_url: str,
    destination: Path,
    components: Optional[List[str]] = None,
    artists: Optional[List[str]] = None,
    proportion: Optional[float] = None,
    offset: int = 0,
    progress_callback = None
) -> SyncStats:
    """Clone dataset from remote source."""
    # Initialize client and stats
    client = WebDAVClient(source_url)
    stats = SyncStats()
    
    try:
        # First download schema
        logger.info("Downloading schema from remote...")
        schema_path = destination / '.blackbird' / 'schema.json'
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create a temporary path for the remote schema
        temp_schema_path = destination / '.blackbird' / 'remote_schema.json'
        if not client.download_file('.blackbird/schema.json', temp_schema_path):
            raise ValueError(f"Failed to download schema from remote. Please check if the WebDAV server at {source_url} is accessible and contains a valid Blackbird dataset.")
            
        # Load remote schema
        remote_schema = DatasetComponentSchema.load(temp_schema_path)
        
        # Create or load local schema
        if schema_path.exists():
            local_schema = DatasetComponentSchema.load(schema_path)
        else:
            # Create a new schema with empty components
            local_schema = DatasetComponentSchema.create(destination)
            local_schema.schema['components'] = {}  # Start with empty components
        
        # Validate requested components against remote schema
        available_components = set(remote_schema.schema['components'].keys())
        if components:
            invalid_components = set(components) - available_components
            if invalid_components:
                error_msg = f"Invalid components: {invalid_components}\nAvailable components are: {sorted(available_components)}"
                raise ValueError(error_msg)
                
            # Update local schema with only the requested components from remote
            for component in components:
                local_schema.schema['components'][component] = remote_schema.schema['components'][component]
        else:
            # If no specific components requested, copy all components from remote
            local_schema.schema['components'] = remote_schema.schema['components'].copy()
            
        # Save the updated local schema
        local_schema.save()
        
        # Clean up temporary schema file
        temp_schema_path.unlink()
        
        # Download index
        logger.info("\nDownloading dataset index...")
        index_path = destination / '.blackbird' / 'index.pickle'
        if not client.download_file('.blackbird/index.pickle', index_path):
            raise ValueError(f"Failed to download index from remote. Please check if the WebDAV server at {source_url} is accessible and contains a valid Blackbird dataset.")
            
        # Load index
        index = DatasetIndex.load(index_path)
        
        # Analyze index contents
        component_counts = defaultdict(int)
        component_patterns = {}
        component_sizes = defaultdict(int)
        for track_info in index.tracks.values():
            for comp_name, file_path in track_info.files.items():
                component_counts[comp_name] += 1
                component_sizes[comp_name] += track_info.file_sizes[file_path]
                if comp_name in remote_schema.schema['components']:
                    component_patterns[comp_name] = remote_schema.schema['components'][comp_name]['pattern']
        
        # Log index statistics
        logger.info(f"\nIndex Statistics:")
        logger.info(f"Total tracks: {len(index.tracks)}")
        logger.info(f"Total artists: {len(index.album_by_artist)}")
        
        if components:
            logger.info("\nRequested components in index:")
            for comp_name in components:
                count = component_counts[comp_name]
                pattern = component_patterns.get(comp_name, "unknown pattern")
                size_gb = component_sizes[comp_name] / (1024*1024*1024)
                logger.info(f"- {comp_name}:")
                logger.info(f"  Pattern: {pattern}")
                logger.info(f"  Files found: {count}")
                logger.info(f"  Total size: {size_gb:.2f} GB")
                if count == 0:
                    logger.warning(f"  WARNING: No files found for component '{comp_name}'")
        
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
        
        # Get list of artists to process
        target_artists = artists if artists else list(index.album_by_artist.keys())
        logger.info(f"\nProcessing artists: {target_artists}")
        
        # Find all files to download based on index
        all_files = []  # List of (file_path, file_size) tuples
        stats.total_files = 0
        stats.total_size = 0
        
        # Process each artist
        for artist in target_artists:
            if artist not in index.album_by_artist:
                logger.warning(f"Artist not found in index: {artist}")
                continue
                
            # Get all tracks for this artist
            for album_path in index.album_by_artist[artist]:
                for track_path in index.track_by_album[album_path]:
                    track = index.tracks[track_path]
                    
                    # Check each requested component
                    target_components = components if components else local_schema.schema['components'].keys()
                    for component in target_components:
                        if component in track.files:
                            file_path = track.files[component]
                            file_size = track.file_sizes[file_path]
                            all_files.append((file_path, file_size))
                            stats.total_files += 1
                            stats.total_size += file_size
        
        # Download files with progress bar
        with tqdm(total=stats.total_size, unit='B', unit_scale=True) as pbar:
            for file_path, file_size in all_files:
                local_path = destination / file_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                if client.download_file(file_path, local_path):
                    stats.downloaded_files += 1
                    stats.downloaded_size += file_size
                    pbar.update(file_size)
                else:
                    stats.failed_files += 1
                    
                if progress_callback:
                    progress_callback(stats.downloaded_size / stats.total_size)
                    
        return stats
        
    except Exception as e:
        logger.error(f"Clone failed: {e}")
        raise

def configure_client(url: str) -> 'WebDAVClient':
    """Configure WebDAV client for remote dataset access.
    
    Args:
        url: WebDAV server URL (e.g. http://localhost:8080)
        
    Returns:
        Configured WebDAV client
    """
    return WebDAVClient(url) 