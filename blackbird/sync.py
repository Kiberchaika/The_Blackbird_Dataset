from pathlib import Path
from typing import List, Optional, Dict, Set
import logging
from enum import Enum
from dataclasses import dataclass
from webdav3.client import Client
from tqdm import tqdm
import os

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