from pathlib import Path
from typing import List, Optional, Dict, Set, Callable
from webdav3.client import Client
import json
import logging
from tqdm import tqdm
from dataclasses import dataclass
from datetime import datetime
import fnmatch
import urllib.parse
from .schema import DatasetComponentSchema, SchemaValidationResult
import itertools

logger = logging.getLogger(__name__)

@dataclass
class SyncState:
    """Track sync state for resumable operations."""
    last_sync: datetime
    synced_files: Set[str]
    failed_files: Dict[str, str]  # file -> error message
    total_bytes: int
    completed_bytes: int

    def save(self, path: Path):
        """Save sync state to file."""
        state = {
            "last_sync": self.last_sync.isoformat(),
            "synced_files": list(self.synced_files),
            "failed_files": self.failed_files,
            "total_bytes": self.total_bytes,
            "completed_bytes": self.completed_bytes
        }
        with open(path, 'w') as f:
            json.dump(state, f, indent=2)

    @classmethod
    def load(cls, path: Path) -> 'SyncState':
        """Load sync state from file."""
        with open(path) as f:
            state = json.load(f)
        return cls(
            last_sync=datetime.fromisoformat(state["last_sync"]),
            synced_files=set(state["synced_files"]),
            failed_files=state["failed_files"],
            total_bytes=state["total_bytes"],
            completed_bytes=state["completed_bytes"]
        )

    @classmethod
    def create_new(cls) -> 'SyncState':
        """Create new sync state."""
        return cls(
            last_sync=datetime.now(),
            synced_files=set(),
            failed_files={},
            total_bytes=0,
            completed_bytes=0
        )

class DatasetSync:
    """Handles dataset synchronization with WebDAV server."""

    def __init__(self, dataset_path: Path):
        """Initialize sync manager.
        
        Args:
            dataset_path: Path to dataset root
        """
        self.dataset_path = Path(dataset_path)
        self.schema = DatasetComponentSchema(dataset_path)
        self.state_file = self.dataset_path / ".blackbird" / "sync_state.json"

    def configure_client(
        self,
        webdav_url: str,
        username: str,
        password: str,
        remote_path: str = "/"
    ) -> Client:
        """Configure WebDAV client.
        
        Args:
            webdav_url: WebDAV server URL
            username: WebDAV username
            password: WebDAV password
            remote_path: Base path on remote server
            
        Returns:
            Configured WebDAV client
        """
        options = {
            'webdav_hostname': webdav_url,
            'webdav_login': username,
            'webdav_password': password,
            'webdav_root': remote_path,
            'disable_check': True,  # Disable initial check which can fail with non-ASCII paths
            'verbose': True,  # Enable verbose logging for debugging
            'chunk_size': 1024 * 1024,  # 1MB chunks for better performance
            'verify': True,  # Verify SSL certificates
            'timeout': 30,  # Increase timeout for large directories
            'header_encoding': 'utf-8',  # Ensure headers are UTF-8 encoded
            'default_file_encoding': 'utf-8'  # Use UTF-8 for file names
        }
        
        client = Client(options)
        
        # Test connection
        try:
            logger.info("Testing WebDAV connection...")
            root_files = client.list()
            logger.info(f"Successfully listed root directory with {len(root_files)} items")
            for file in root_files[:5]:  # Log first few items for verification
                logger.debug(f"Root item: {urllib.parse.unquote(file)}")
        except Exception as e:
            logger.error(f"Failed to test WebDAV connection: {str(e)}", exc_info=True)
            raise
        
        return client

    def _get_remote_schema(self, client: Client) -> dict:
        """Get schema from remote server.
        
        Args:
            client: WebDAV client
            
        Returns:
            Remote schema dictionary
        """
        try:
            # Download remote schema
            remote_schema_path = ".blackbird/schema.json"
            local_temp = self.dataset_path / ".blackbird" / "remote_schema.json.tmp"
            
            # Download schema using raw path - let client handle encoding
            client.download_sync(remote_schema_path, str(local_temp))
            
            # Load schema
            with open(local_temp) as f:
                remote_schema = json.load(f)
                
            # Clean up temp file
            local_temp.unlink()
            return remote_schema
            
        except Exception as e:
            logger.error(f"Failed to get remote schema: {str(e)}")
            raise ValueError("Remote schema not available") from e

    def _merge_schema(self, remote_schema: dict, components: Optional[List[str]] = None) -> SchemaValidationResult:
        """Merge remote schema components into local schema.
        
        Only imports requested components from remote schema.
        Local schema retains its existing component definitions.
        
        Args:
            remote_schema: Remote schema dictionary
            components: Optional list of components to merge (if None, merges all)
            
        Returns:
            Validation result of merged schema
        """
        # Validate remote schema version
        if remote_schema.get("version") != self.schema.schema.get("version"):
            raise ValueError("Schema version mismatch")
            
        # Get requested components from remote
        components_to_merge = (
            components if components is not None
            else remote_schema["components"].keys()
        )
        
        for name in components_to_merge:
            if name not in remote_schema["components"]:
                continue  # Skip if component doesn't exist in remote
                
            config = remote_schema["components"][name]
            if name not in self.schema.schema["components"]:
                # Add new component
                result = self.schema.add_component(
                    name=name,
                    pattern=config["pattern"],
                    required=config.get("required", False),
                    multiple=config.get("multiple", False),
                    description=config.get("description"),
                    dry_run=True,  # Validate first
                    skip_file_validation=True  # Skip file validation during merge
                )
                if result.is_valid:
                    self.schema.schema["components"][name] = config
                else:
                    logger.warning(f"Skipping invalid component {name}: {result.errors}")
                    
        # Save updated schema
        self.schema.save()
        
        # Validate merged schema
        return self.schema.validate(skip_file_validation=True)  # Skip file validation during merge

    def sync(
        self,
        client: Client,
        components: Optional[List[str]] = None,
        artists: Optional[List[str]] = None,
        resume: bool = True,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> SyncState:
        """Sync dataset with remote server.
        
        Args:
            client: Configured WebDAV client
            components: List of components to sync (default: sync all components)
            artists: Optional list of artists to sync (if None, syncs all artists)
            resume: Whether to resume previous sync
            progress_callback: Optional callback for progress updates
            
        Returns:
            Final sync state
        """
        # First, get and merge remote schema
        if progress_callback:
            progress_callback("Getting remote schema...")
            
        remote_schema = self._get_remote_schema(client)
        logger.info("Got remote schema")
        
        # If no components specified, use all components from remote schema
        if components is None:
            components = list(remote_schema["components"].keys())
            
        schema_result = self._merge_schema(remote_schema, components)
        logger.info(f"Merged schema with components: {components}")
        
        if not schema_result.is_valid:
            raise ValueError(f"Schema validation failed: {schema_result.errors}")
            
        # Load or create sync state
        state = (
            SyncState.load(self.state_file)
            if resume and self.state_file.exists()
            else SyncState.create_new()
        )

        # Validate components exist in schema
        invalid = set(components) - set(self.schema.schema["components"].keys())
        if invalid:
            raise ValueError(f"Invalid components: {invalid}")

        # Get patterns to sync
        patterns = [
            self.schema.schema["components"][c]["pattern"]
            for c in components
        ]
        logger.info(f"Using patterns for sync: {patterns}")
        exclude_patterns = self.schema.schema["sync"]["exclude_patterns"]
        logger.info(f"Using exclude patterns: {exclude_patterns}")

        # Find all files to sync
        if progress_callback:
            progress_callback("Finding files to sync...")
        
        files_to_sync = set()
        try:
            logger.info("Listing root directory...")
            root_items = client.list()
            # Items are already URL encoded, decode for display
            root_items = [urllib.parse.unquote(f) for f in root_items]
            
            # Filter artists if specified
            if artists:
                root_items = [item for item in root_items 
                            if any(artist in item for artist in artists)]
                logger.info(f"Filtered to requested artists: {artists}")
            
            # Log only first 20 artists for clarity
            artist_dirs = [item for item in root_items if item.endswith('/')]
            logger.info(f"Found artists (showing first 20): {artist_dirs[:20]}")
            
        except Exception as e:
            logger.error(f"Failed to list root directory: {str(e)}", exc_info=True)
            root_items = []
        
        # For each artist directory, list its albums
        for artist_dir in root_items:
            if not artist_dir.endswith('/'):  # Skip non-directory items
                continue
                
            # Skip if not in requested artists
            if artists and not any(artist in artist_dir for artist in artists):
                continue
                
            logger.info(f"Processing artist: {artist_dir}")
            try:
                # List albums in artist directory
                albums = client.list(artist_dir.rstrip('/'))
                albums = [urllib.parse.unquote(a) for a in albums]
                # Log only first 5 albums
                logger.debug(f"Albums in {artist_dir} (first 5): {[a for a in albums if a.endswith('/')][:5]}")
                
                # For each album, look for matching files
                for album in albums:
                    if not album.endswith('/'):
                        continue
                        
                    album_path = f"{artist_dir.rstrip('/')}/{album.rstrip('/')}"
                    try:
                        # List all files in album
                        album_files = client.list(album_path)
                        album_files = [urllib.parse.unquote(f) for f in album_files]
                        
                        # Match patterns at the album level
                        for pattern in patterns:
                            matching_files = {
                                f"{album_path}/{f}" for f in album_files
                                if fnmatch.fnmatch(f, pattern)
                            }
                            logger.info(f"Pattern {pattern} matched {len(matching_files)} files in album {album_path}")
                            if matching_files:
                                # Log only first 5 matching files
                                logger.debug(f"First 5 matched files: {list(matching_files)[:5]}")
                            files_to_sync.update(matching_files)
                    except Exception as e:
                        logger.error(f"Failed to list album {album_path}: {str(e)}", exc_info=True)
                        continue
                        
            except Exception as e:
                logger.error(f"Failed to list albums for artist {artist_dir}: {str(e)}", exc_info=True)
                continue

        logger.info(f"Total files to sync: {len(files_to_sync)}")
        if files_to_sync:
            # Log only first 5 files to sync
            logger.debug(f"First 5 files to sync: {list(files_to_sync)[:5]}")
        else:
            logger.warning("No files found to sync!")
            logger.debug(f"Current patterns: {patterns}")

        # Apply exclusions
        for pattern in exclude_patterns:
            files_to_sync = {
                f for f in files_to_sync
                if not any(fnmatch.fnmatch(str(f), p) for p in exclude_patterns)
            }

        # Remove already synced files if resuming
        if resume:
            files_to_sync = {
                f for f in files_to_sync
                if str(f) not in state.synced_files
            }

        # Calculate total size
        if progress_callback:
            progress_callback("Calculating total size...")
        
        total_size = 0
        for f in files_to_sync:
            try:
                # Let client handle path encoding
                file_info = client.info(f)
                total_size += int(file_info["size"])
            except Exception as e:
                logger.error(f"Failed to get info for file {f}: {str(e)}", exc_info=True)
                continue
        
        state.total_bytes = total_size + state.completed_bytes

        # Sync files
        if progress_callback:
            progress_callback(f"Syncing {len(files_to_sync)} files...")

        for file in tqdm(
            files_to_sync,
            desc="Syncing files",
            disable=progress_callback is None
        ):
            try:
                # Create parent directories
                local_path = self.dataset_path / file
                local_path.parent.mkdir(parents=True, exist_ok=True)

                # Get file info first to verify it exists
                try:
                    # Let client handle path encoding
                    logger.debug(f"Getting info for file: {file}")
                    file_info = client.info(file)
                    logger.debug(f"File info: {file_info}")
                except Exception as e:
                    logger.error(f"Failed to get info for file {file}: {str(e)}")
                    continue

                # Download file
                logger.info(f"Downloading {file}...")
                client.download_sync(
                    remote_path=file,
                    local_path=str(local_path)
                )

                # Verify download
                if not local_path.exists():
                    raise FileNotFoundError(f"Downloaded file not found: {local_path}")
                if local_path.stat().st_size == 0:
                    raise ValueError(f"Downloaded file is empty: {local_path}")

                # Update state
                state.synced_files.add(file)
                state.completed_bytes += local_path.stat().st_size
                state.last_sync = datetime.now()
                state.save(self.state_file)

            except Exception as e:
                logger.error(f"Failed to sync {file}: {str(e)}", exc_info=True)
                state.failed_files[file] = str(e)

        return state

def get_remote_schema(schema_path: str) -> dict:
    """Get schema from a remote or local path.
    
    Args:
        schema_path: Path to schema file
        
    Returns:
        Schema dictionary
    """
    with open(schema_path) as f:
        return json.load(f)

def sync_components(
    dataset_root: str,
    components: List[str],
    artists: Optional[List[str]] = None
) -> None:
    """Sync specific components for given artists.
    
    Args:
        dataset_root: Path to dataset root
        components: List of components to sync
        artists: Optional list of artists to sync (if None, syncs all)
    """
    sync = DatasetSync(Path(dataset_root))
    client = sync.configure_client(
        webdav_url="http://localhost:8080",
        username="user",
        password="test123"
    )
    sync.sync(client, components=components)
