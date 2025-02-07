from pathlib import Path
from typing import List, Optional, Dict, Set, Callable
from webdav3.client import Client
import json
import logging
from tqdm import tqdm
from dataclasses import dataclass
from datetime import datetime
import fnmatch
from .schema import DatasetComponentSchema

logger = logging.getLogger(__name__)

@dataclass
class SyncState:
    """Track sync state for resumable operations."""
    last_sync: datetime
    synced_files: Set[str]  # Files successfully downloaded
    failed_files: Dict[str, str]  # file -> error message
    total_bytes: int  # Total bytes to download
    completed_bytes: int  # Bytes downloaded so far

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
    """Handles dataset synchronization from remote WebDAV server."""

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

    def _update_schema_from_remote(
        self,
        client: Client,
        components: Optional[List[str]] = None
    ):
        """Update local schema with component definitions from remote.
        
        Args:
            client: WebDAV client
            components: Optional list of components to update (default: all)
        """
        try:
            # Download remote schema
            remote_schema_path = ".blackbird/schema.json"
            local_temp_path = self.dataset_path / ".blackbird" / "remote_schema.json"
            
            client.download_sync(remote_schema_path, str(local_temp_path))
            
            with open(local_temp_path) as f:
                remote_schema = json.load(f)
            
            # Update local schema with remote components
            remote_components = remote_schema["components"]
            if components:
                # Only update specified components
                for component in components:
                    if component in remote_components:
                        self.schema.schema["components"][component] = remote_components[component]
            else:
                # Update all components
                self.schema.schema["components"].update(remote_components)
            
            self.schema.save()
            local_temp_path.unlink()  # Clean up temp file
            
        except Exception as e:
            logger.error(f"Failed to update schema from remote: {str(e)}")
            raise

    def sync_from_remote(
        self,
        client: Client,
        components: Optional[List[str]] = None,
        resume: bool = True,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> SyncState:
        """Sync dataset from remote server.
        
        Args:
            client: Configured WebDAV client
            components: List of components to sync (default: use schema defaults)
            resume: Whether to resume previous sync
            progress_callback: Optional callback for progress updates
            
        Returns:
            Final sync state
        """
        # First update schema from remote
        if progress_callback:
            progress_callback("Updating schema from remote...")
        self._update_schema_from_remote(client, components)

        # Load or create sync state
        state = (
            SyncState.load(self.state_file)
            if resume and self.state_file.exists()
            else SyncState.create_new()
        )

        # Use default components from schema if none specified
        if components is None:
            components = self.schema.schema["sync"]["default_components"]

        # Get patterns to sync from updated schema
        patterns = []
        missing_components = []
        for component in components:
            if component in self.schema.schema["components"]:
                patterns.append(self.schema.schema["components"][component]["pattern"])
            else:
                missing_components.append(component)
                
        if missing_components:
            raise ValueError(
                f"Components not found in remote schema: {', '.join(missing_components)}. "
                f"Available components: {', '.join(self.schema.schema['components'].keys())}"
            )

        exclude_patterns = self.schema.schema["sync"]["exclude_patterns"]

        # Find remote files to sync
        if progress_callback:
            progress_callback("Finding remote files to sync...")
        
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
                if f not in state.synced_files
            }

        # Calculate total size (if remote server provides this info)
        if progress_callback:
            progress_callback("Preparing to download files...")

        # Download files
        if progress_callback:
            progress_callback(f"Downloading {len(files_to_sync)} files...")

        for remote_file in tqdm(
            files_to_sync,
            desc="Downloading files",
            disable=progress_callback is None
        ):
            try:
                # Create parent directories
                local_path = self.dataset_path / remote_file
                local_path.parent.mkdir(parents=True, exist_ok=True)

                # Download file
                client.download_sync(
                    remote_path=remote_file,
                    local_path=str(local_path)
                )

                # Update state
                state.synced_files.add(remote_file)
                if local_path.exists():  # Update completed bytes if we can
                    state.completed_bytes += local_path.stat().st_size
                state.last_sync = datetime.now()
                state.save(self.state_file)

            except Exception as e:
                logger.error(f"Failed to sync {remote_file}: {str(e)}")
                state.failed_files[remote_file] = str(e)

        return state

    def _update_schema_for_new_file(self, file_path: str):
        """Update schema if file represents a new component type."""
        # Try to identify component from filename
        for suffix in ['_instrumental', '_vocals_noreverb', '_mir']:
            if suffix in file_path:
                component_name = suffix.lstrip('_')
                if component_name not in self.schema.schema["components"]:
                    # Add new component to schema
                    pattern = f"*{suffix}.*"
                    self.schema.add_component(
                        name=component_name,
                        pattern=pattern,
                        required=False,
                        description=f"Component discovered from remote: {pattern}"
                    )
                break
