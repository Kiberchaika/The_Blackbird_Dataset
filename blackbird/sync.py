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
        options = {
            'webdav_hostname': f"http://{host}",
            'webdav_root': parsed.path,
            'webdav_timeout': 30
        }
        
        if username and password:
            options['webdav_login'] = username
            options['webdav_password'] = password
            
        self.client = webdav.Client(options)
        
    def list_files(self, path: str = '') -> List[Dict[str, str]]:
        """List files in remote directory.
        
        Args:
            path: Remote directory path
            
        Returns:
            List of file info dictionaries with 'path' and 'size' keys
        """
        try:
            files = self.client.list(path, get_info=True)
            return [
                {
                    'path': f['path'],
                    'size': int(f.get('size', 0))
                }
                for f in files
                if not f['path'].endswith('/')  # Skip directories
            ]
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
            
            # Download file
            self.client.download_sync(
                remote_path=remote_path,
                local_path=str(local_path)
            )
            return True
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
    """Clone dataset from remote source.
    
    Args:
        source_url: Remote dataset URL
        destination: Local destination path
        components: Optional list of components to clone
        artists: Optional list of artists to clone
        proportion: Optional proportion of dataset to clone (0-1)
        offset: Offset for proportion-based cloning
        progress_callback: Optional callback for progress updates
        
    Returns:
        Sync statistics
    """
    # Initialize client and stats
    client = WebDAVClient(source_url)
    stats = SyncStats()
    
    try:
        # First download schema
        logger.info("Downloading schema from remote...")
        schema_path = destination / '.blackbird' / 'schema.json'
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        
        if not client.download_file('.blackbird/schema.json', schema_path):
            raise ValueError("Failed to download schema from remote")
            
        # Load schema and show available components
        schema = DatasetComponentSchema(destination)
        logger.info("\nAvailable components:")
        for comp_name, comp_info in schema.schema['components'].items():
            logger.info(f"- {comp_name} ({comp_info['pattern']})")
            
        # Download index
        logger.info("\nDownloading dataset index...")
        index_path = destination / '.blackbird' / 'index.pickle'
        if not client.download_file('.blackbird/index.pickle', index_path):
            raise ValueError("Failed to download index from remote")
            
        # Load index
        index = DatasetIndex.load(index_path)
        logger.info(f"Index loaded successfully:")
        logger.info(f"Total tracks: {len(index.tracks)}")
        logger.info(f"Total artists: {len(index.album_by_artist)}")
        
        # Validate components if specified
        if components:
            invalid_components = []
            for component in components:
                if component not in schema.schema['components']:
                    invalid_components.append(component)
                    # Find similar component names
                    similar = get_close_matches(component, schema.schema['components'].keys(), n=1)
                    if similar:
                        click.secho(f"Error: Unknown component '{component}'. Did you mean '{similar[0]}'?", fg="red")
                    else:
                        click.secho(f"Error: Unknown component '{component}'. No similar components found.", fg="red")
                        
            if invalid_components:
                click.secho("\nAvailable components:", fg="yellow")
                for comp_name, comp_info in schema.schema['components'].items():
                    click.echo(f"- {comp_name} ({comp_info['pattern']})")
                raise ValueError(f"Invalid components: {', '.join(invalid_components)}")
                
        # Validate artists if specified
        if artists:
            invalid_artists = []
            for artist in artists:
                # First try exact match
                exact_matches = index.search_by_artist(artist, case_sensitive=True)
                if exact_matches:
                    continue
                    
                # Then try case-insensitive match
                case_insensitive_matches = index.search_by_artist(artist, case_sensitive=False)
                if case_insensitive_matches:
                    click.secho(f"Warning: Using case-insensitive match for artist '{artist}': {case_insensitive_matches[0]}", fg="yellow")
                    continue
                    
                # Finally try fuzzy search
                fuzzy_matches = index.search_by_artist(artist, fuzzy_search=True)
                if fuzzy_matches:
                    click.secho(f"Error: Unknown artist '{artist}'. Did you mean '{fuzzy_matches[0]}'?", fg="red")
                else:
                    click.secho(f"Error: Unknown artist '{artist}'. No similar artists found.", fg="red")
                invalid_artists.append(artist)
                
            if invalid_artists:
                click.secho("\nAvailable artists:", fg="yellow")
                for artist in sorted(index.album_by_artist.keys())[:10]:  # Show first 10 artists
                    click.echo(f"- {artist}")
                if len(index.album_by_artist) > 10:
                    click.echo("... and more")
                raise ValueError(f"Invalid artists: {', '.join(invalid_artists)}")
        
        # List all files
        logger.info("\nListing remote files...")
        all_files = client.list_files()
        
        # Filter files based on components
        if components:
            valid_patterns = []
            for component in components:
                pattern = schema.schema['components'][component]['pattern']
                valid_patterns.append(pattern)
                    
            filtered_files = []
            for file_info in all_files:
                if any(fnmatch.fnmatch(file_info['path'], pattern) for pattern in valid_patterns):
                    filtered_files.append(file_info)
            all_files = filtered_files
            
        # Filter by artists if specified
        if artists:
            artist_patterns = [f"{artist}/*" for artist in artists]
            filtered_files = []
            for file_info in all_files:
                if any(fnmatch.fnmatch(file_info['path'], pattern) for pattern in artist_patterns):
                    filtered_files.append(file_info)
            all_files = filtered_files
            
        # Apply proportion and offset if specified
        if proportion is not None:
            start_idx = int(len(all_files) * offset)
            end_idx = int(len(all_files) * (offset + proportion))
            all_files = all_files[start_idx:end_idx]
            
        # Calculate total size
        stats.total_files = len(all_files)
        stats.total_size = sum(f['size'] for f in all_files)
        
        logger.info(f"\nStarting download:")
        logger.info(f"Files to download: {stats.total_files}")
        logger.info(f"Total size: {stats.total_size / (1024*1024*1024):.2f} GB")
        
        # Download files with progress bar
        with tqdm(total=stats.total_size, unit='B', unit_scale=True) as pbar:
            for file_info in all_files:
                remote_path = file_info['path']
                local_path = destination / remote_path
                
                if client.download_file(remote_path, local_path):
                    stats.downloaded_files += 1
                    stats.downloaded_size += file_info['size']
                    pbar.update(file_info['size'])
                else:
                    stats.failed_files += 1
                    
                if progress_callback:
                    progress_callback(stats.downloaded_size / stats.total_size)
                    
        return stats
        
    except Exception as e:
        logger.error(f"Clone failed: {e}")
        raise 