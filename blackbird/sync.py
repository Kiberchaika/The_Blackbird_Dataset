from pathlib import Path
from typing import List, Optional, Dict, Set, Callable
from webdav3.client import Client
import json
import logging
from tqdm import tqdm
from dataclasses import dataclass
from datetime import datetime
import fnmatch
from .schema import DatasetComponentSchema, SchemaValidationResult

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
            'webdav_root': remote_path
        }
        return Client(options)

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

    def _merge_schema(self, remote_schema: dict) -> SchemaValidationResult:
        """Merge remote schema components into local schema.
        
        Only imports new/updated components from remote schema.
        Local schema retains its existing component definitions.
        
        Args:
            remote_schema: Remote schema dictionary
            
        Returns:
            Validation result of merged schema
        """
        # Validate remote schema version
        if remote_schema.get("version") != self.schema.schema.get("version"):
            raise ValueError("Schema version mismatch")
            
        # Get new/updated components from remote
        for name, config in remote_schema["components"].items():
            if name not in self.schema.schema["components"]:
                # Add new component
                result = self.schema.add_component(
                    name=name,
                    pattern=config["pattern"],
                    required=config.get("required", False),
                    multiple=config.get("multiple", False),
                    description=config.get("description"),
                    dry_run=True  # Validate first
                )
                if result.is_valid:
                    self.schema.schema["components"][name] = config
                else:
                    logger.warning(f"Skipping invalid component {name}: {result.errors}")
                    
        # Save updated schema
        self.schema.save()
        
        # Validate merged schema
        return self.schema.validate()

    def sync(
        self,
        client: Client,
        components: Optional[List[str]] = None,
        resume: bool = True,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> SyncState:
        """Sync dataset with remote server.
        
        Args:
            client: Configured WebDAV client
            components: List of components to sync (default: use schema defaults)
            resume: Whether to resume previous sync
            progress_callback: Optional callback for progress updates
            
        Returns:
            Final sync state
        """
        # First, get and merge remote schema
        if progress_callback:
            progress_callback("Getting remote schema...")
            
        remote_schema = self._get_remote_schema(client)
        schema_result = self._merge_schema(remote_schema)
        
        if not schema_result.is_valid:
            raise ValueError(f"Schema validation failed: {schema_result.errors}")
            
        # Load or create sync state
        state = (
            SyncState.load(self.state_file)
            if resume and self.state_file.exists()
            else SyncState.create_new()
        )

        # Use default components from schema if none specified
        if components is None:
            components = self.schema.schema["sync"]["default_components"]

        # Validate components exist in schema
        invalid = set(components) - set(self.schema.schema["components"].keys())
        if invalid:
            raise ValueError(f"Invalid components: {invalid}")

        # Get patterns to sync
        patterns = [
            self.schema.schema["components"][c]["pattern"]
            for c in components
        ]
        exclude_patterns = self.schema.schema["sync"]["exclude_patterns"]

        # Find all files to sync
        if progress_callback:
            progress_callback("Finding files to sync...")
        
        files_to_sync = set()
        for pattern in patterns:
            remote_files = client.list(pattern)
            files_to_sync.update(remote_files)

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
        
        total_size = sum(
            client.info(f)["size"]
            for f in files_to_sync
        )
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

                # Download file
                client.download_sync(
                    remote_path=file,
                    local_path=str(local_path)
                )

                # Update state
                state.synced_files.add(file)
                state.completed_bytes += local_path.stat().st_size
                state.last_sync = datetime.now()
                state.save(self.state_file)

            except Exception as e:
                logger.error(f"Failed to sync {file}: {str(e)}")
                state.failed_files[file] = str(e)

        return state
