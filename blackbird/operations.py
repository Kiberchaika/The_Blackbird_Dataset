import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, TypedDict, Union

logger = logging.getLogger(__name__)

OPERATION_STATE_FILENAME_PREFIX = "operation"

OperationStatus = Union[Literal["pending", "done"], str]  # Use str for "failed: <reason>"

class OperationState(TypedDict):
    operation_type: Literal["sync", "move"]
    timestamp: float
    source: str  # webdav URL for sync, source location name for move
    target_location: str # Target location name
    components: Optional[List[str]] # Only for sync
    files: Dict[int, OperationStatus] # file_hash -> status


def get_state_file_path(blackbird_dir: Path, operation_type: str, timestamp: float) -> Path:
    """Generates the path for an operation state file."""
    filename = f"{OPERATION_STATE_FILENAME_PREFIX}_{operation_type}_{timestamp:.0f}.json"
    return blackbird_dir / filename

def create_operation_state(
    blackbird_dir: Path,
    operation_type: Literal["sync", "move"],
    source: str,
    target_location: str,
    file_hashes: List[int],
    components: Optional[List[str]] = None,
) -> Path:
    """Creates and saves the initial operation state file."""
    timestamp = time.time()
    state_file_path = get_state_file_path(blackbird_dir, operation_type, timestamp)
    
    state: OperationState = {
        "operation_type": operation_type,
        "timestamp": timestamp,
        "source": source,
        "target_location": target_location,
        "components": components if operation_type == "sync" else None,
        "files": {file_hash: "pending" for file_hash in file_hashes},
    }
    
    try:
        with open(state_file_path, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(f"Created operation state file: {state_file_path}")
        return state_file_path
    except IOError as e:
        logger.error(f"Failed to create operation state file {state_file_path}: {e}")
        raise # Re-raise the exception after logging

def load_operation_state(state_file_path: Path) -> Optional[OperationState]:
    """Loads an operation state file."""
    if not state_file_path.exists():
        logger.warning(f"Operation state file not found: {state_file_path}")
        return None
    try:
        with open(state_file_path, "r") as f:
            state = json.load(f)
            # Basic validation (can be expanded)
            if not all(k in state for k in ["operation_type", "timestamp", "source", "target_location", "files"]):
                 raise ValueError("Invalid state file format.")
            # Cast files keys back to int
            state["files"] = {int(k): v for k, v in state["files"].items()}
            return state # type: ignore # Trusting basic validation for now
    except (IOError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to load or parse operation state file {state_file_path}: {e}")
        return None # Treat load failure as non-resumable

def update_operation_state_file(
    state_file_path: Path,
    file_hash: int,
    status: OperationStatus,
):
    """Updates the status of a single file hash in the state file."""
    # This is not atomic but should be sufficient for now.
    # For higher concurrency, locking or transactional updates might be needed.
    try:
        current_state = load_operation_state(state_file_path)
        if not current_state:
            # Logged in load_operation_state
            return

        if file_hash not in current_state["files"]:
            logger.warning(f"File hash {file_hash} not found in state file {state_file_path}. Skipping update.")
            return

        current_state["files"][file_hash] = status

        # Rewrite the file
        with open(state_file_path, "w") as f:
            # Convert keys back to string for JSON
            state_to_save = current_state.copy()
            state_to_save["files"] = {str(k): v for k, v in current_state["files"].items()}
            json.dump(state_to_save, f, indent=2)
            
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to update operation state file {state_file_path} for hash {file_hash}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error updating state file {state_file_path} for hash {file_hash}: {e}")


def delete_operation_state(state_file_path: Path):
    """Deletes the operation state file."""
    try:
        if state_file_path.exists():
            state_file_path.unlink()
            logger.info(f"Deleted operation state file: {state_file_path}")
        else:
            logger.warning(f"Attempted to delete non-existent state file: {state_file_path}")
    except OSError as e:
        logger.error(f"Failed to delete operation state file {state_file_path}: {e}")

def find_latest_state_file(blackbird_dir: Path, operation_type: Literal["sync", "move"]) -> Optional[Path]:
    """Finds the most recent state file for a given operation type."""
    pattern = f"{OPERATION_STATE_FILENAME_PREFIX}_{operation_type}_*.json"
    state_files = sorted(
        blackbird_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if state_files:
        return state_files[0]
    return None 