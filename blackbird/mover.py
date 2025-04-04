import os
import shutil
import time
import logging
from pathlib import Path
from typing import List, Optional, Dict, Tuple

from .dataset import Dataset
from . import operations
from .utils import format_size
from .locations import SymbolicPathError

logger = logging.getLogger(__name__)

def move_data(
    dataset: Dataset,
    source_location_name: str,
    target_location_name: str,
    size_limit_gb: Optional[float] = None,
    specific_folders: Optional[List[str]] = None,
    dry_run: bool = False,
    operation_state: Optional[Dict] = None,
    state_file_path: Optional[Path] = None,
) -> Dict:
    """
    Moves data between storage locations within the dataset.

    Args:
        dataset: The Dataset instance.
        source_location_name: The name of the source storage location.
        target_location_name: The name of the target storage location.
        size_limit_gb: Optional limit in GB for the amount of data to move.
        specific_folders: Optional list of folder paths relative to the source
                          location root to move. If None, considers all data
                          in the source location.
        dry_run: If True, only simulates the move and prints actions.
        operation_state: If provided, resumes a previous move operation using
                         this loaded state.
        state_file_path: If provided (when resuming), the path to the state file.

    Returns:
        A dictionary containing statistics about the move operation.
    """
    all_locations = dataset.locations.get_all_locations()
    if source_location_name not in all_locations:
        raise ValueError(f"Source location '{source_location_name}' not found in dataset configuration.")
    if target_location_name not in all_locations:
        raise ValueError(f"Target location '{target_location_name}' not found in dataset configuration.")
    if source_location_name == target_location_name:
        raise ValueError("Source and target locations cannot be the same.")

    if dataset.index is None:
        raise RuntimeError("Dataset index not loaded. Please run 'reindex' first.")

    logger.info(f"Starting move from '{source_location_name}' to '{target_location_name}'.")
    if dry_run:
        logger.warning("Performing a DRY RUN. No files will actually be moved.")

    # --- 1. Identify candidate files ---
    candidate_files: List[Tuple[int, str, int]] = [] # (hash, symbolic_path, size)
    total_source_size = 0
    logger.debug("Identifying candidate files...")

    # Normalize specific_folders to ensure they don't have leading/trailing slashes
    normalized_folders = None
    if specific_folders:
        normalized_folders = [folder.strip('/') for folder in specific_folders]
        logger.info(f"Filtering by specific folders: {normalized_folders}")

    for hash_val, (symbolic_path, size) in dataset.index.file_info_by_hash.items():
        try:
            file_location_name = symbolic_path.split('/', 1)[0]
        except IndexError:
            logger.warning(f"Skipping file with invalid symbolic path format: {symbolic_path}")
            continue

        if file_location_name == source_location_name:
            total_source_size += size
            # Check against specific folders if provided
            if normalized_folders:
                try:
                    rel_path_in_loc = symbolic_path.split('/', 1)[1]
                    if not any(rel_path_in_loc.startswith(folder + '/') or rel_path_in_loc == folder
                               for folder in normalized_folders):
                        continue # Skip if not in specified folders
                except IndexError:
                    # Should not happen if split worked before, but handle defensively
                    logger.warning(f"Could not extract relative path from: {symbolic_path}")
                    continue
            candidate_files.append((hash_val, symbolic_path, size))

    logger.info(f"Found {len(candidate_files)} candidate files in '{source_location_name}' (Total size: {format_size(total_source_size)}).")
    if not candidate_files:
        logger.warning("No candidate files found matching the criteria. Nothing to move.")
        return {"moved_files": 0, "failed_files": 0, "skipped_files": 0, "total_bytes_moved": 0, "target_size_gb": 0}

    # --- 2. Apply size limit ---
    files_to_process_info: List[Tuple[int, str, int]] = []
    target_size_bytes = 0
    if size_limit_gb is not None:
        size_limit_bytes = size_limit_gb * (1024**3)
        logger.info(f"Applying size limit: {size_limit_gb:.2f} GB ({format_size(size_limit_bytes)})")
        current_size = 0
        # Sort by path to implicitly group artists/albums together somewhat
        candidate_files.sort(key=lambda x: x[1])
        for hash_val, symbolic_path, size in candidate_files:
            if current_size + size <= size_limit_bytes:
                files_to_process_info.append((hash_val, symbolic_path, size))
                current_size += size
            else:
                # Stop adding once limit is reached or exceeded by the next file
                break
        target_size_bytes = current_size
        logger.info(f"Selected {len(files_to_process_info)} files to meet size limit (Actual size: {format_size(target_size_bytes)}).")
    else:
        files_to_process_info = candidate_files
        target_size_bytes = sum(size for _, _, size in files_to_process_info)
        logger.info(f"No size limit applied. Processing all {len(files_to_process_info)} candidate files (Total size: {format_size(target_size_bytes)}).")

    if not files_to_process_info:
        logger.warning("No files selected after applying size limit. Nothing to move.")
        return {"moved_files": 0, "failed_files": 0, "skipped_files": 0, "total_bytes_moved": 0, "target_size_gb": size_limit_gb}


    # --- 3. Prepare Operation State ---
    is_resuming = operation_state is not None
    current_state_files: Dict[int, str] = {}

    if not is_resuming:
        initial_state_files = {hash_val: "pending" for hash_val, _, _ in files_to_process_info}
        if not dry_run:
            state_file_path = operations.create_operation_state(
                blackbird_dir=dataset.path / ".blackbird",
                operation_type="move",
                source=source_location_name,
                target_location=target_location_name,
                file_hashes=[h for h, _, _ in files_to_process_info]
            )
            logger.info(f"Created operation state file: {state_file_path}")
            current_state_files = initial_state_files
        else:
            state_file_path = None # No state file for dry run
            current_state_files = {hash_val: "skipped (dry run)" for hash_val, _, _ in files_to_process_info}

    else:
        # Resuming
        if not state_file_path:
            state_file_path = operations.find_latest_state_file(dataset.path / ".blackbird", "move")
            if not state_file_path:
                raise ValueError("Resume requested, but no operation state provided and no state file found.")
            logger.info(f"Resuming from inferred state file: {state_file_path}")
            operation_state = operations.load_operation_state(state_file_path) # Reload state

        logger.info(f"Resuming move operation from state file: {state_file_path}")
        current_state_files = operation_state['files']
        # Filter files_to_process_info to only include those needing action
        hashes_in_state = set(current_state_files.keys())
        original_hashes = {h for h, _, _ in files_to_process_info}

        # Check consistency: ensure hashes in state match hashes calculated for the move parameters
        if hashes_in_state != original_hashes:
             logger.warning(f"State file hash count ({len(hashes_in_state)}) differs from "
                           f"currently calculated files ({len(original_hashes)}). "
                           "Parameters might have changed or index updated. Resuming based on state file content.")
             # Rebuild files_to_process based on state file's pending/failed hashes
             files_to_process_info_resuming = []
             processed_hashes = set()
             for hash_val_state, status in current_state_files.items():
                 if status == 'pending' or status.startswith('failed'):
                      try:
                          # Fetch info from index using hash from state
                           file_info = dataset.index.get_file_info_by_hash(hash_val_state)
                           if file_info:
                               files_to_process_info_resuming.append((hash_val_state, file_info[0], file_info[1]))
                               processed_hashes.add(hash_val_state)
                           else:
                               logger.error(f"Hash {hash_val_state} from state file not found in current index. Cannot resume this file.")
                               current_state_files[hash_val_state] = f"failed: Hash not found in current index" # Mark as failed
                      except KeyError:
                           logger.error(f"Hash {hash_val_state} from state file not found in current index (KeyError). Cannot resume this file.")
                           current_state_files[hash_val_state] = f"failed: Hash not found in current index" # Mark as failed

             files_to_process_info = files_to_process_info_resuming
             if not files_to_process_info:
                  logger.warning("No pending or failed files found in state to resume.")
                  if all(s == 'done' for s in current_state_files.values()) and state_file_path:
                      operations.delete_operation_state(state_file_path)
                      logger.info("Deleted completed operation state file.")
                  return {"moved_files": 0, "failed_files": 0, "skipped_files": 0, "total_bytes_moved": 0, "target_size_gb": size_limit_gb}

        else:
             # Filter normally if hashes match
             files_to_process_info = [
                 (h, p, s) for h, p, s in files_to_process_info
                 if current_state_files.get(h, "pending") != "done"
            ]
             logger.info(f"Found {len(files_to_process_info)} files pending or failed in state file.")


    # --- 4. Perform Move ---
    moved_count = 0
    failed_count = 0
    skipped_count = 0
    total_bytes_moved = 0

    if dry_run:
        for _, symbolic_path, size in files_to_process_info:
             target_symbolic_path = target_location_name + '/' + symbolic_path.split('/', 1)[1]
             logger.info(f"DRY RUN: Would move {symbolic_path} ({format_size(size)}) to {target_symbolic_path}")
             skipped_count += 1
        logger.info("Dry run complete.")
        return {"moved_files": 0, "failed_files": 0, "skipped_files": skipped_count, "total_bytes_moved": 0, "target_size_gb": size_limit_gb}

    total_files_to_move = len(files_to_process_info)
    logger.info(f"Starting actual move of {total_files_to_move} files...")

    for i, (hash_val, source_symbolic_path, size) in enumerate(files_to_process_info):
        progress = f"({i + 1}/{total_files_to_move})"
        if current_state_files.get(hash_val) == "done": # Should have been filtered, but double check
            logger.debug(f"{progress} Skipping already completed file: {source_symbolic_path}")
            continue

        try:
            abs_source_path = dataset.resolve_path(source_symbolic_path)
            # Construct target symbolic path
            relative_path = source_symbolic_path.split('/', 1)[1]
            target_symbolic_path = f"{target_location_name}/{relative_path}"
            abs_target_path = dataset.resolve_path(target_symbolic_path)

            # Ensure target directory exists
            abs_target_path.parent.mkdir(parents=True, exist_ok=True)

            # Perform the move
            logger.debug(f"{progress} Moving {abs_source_path} -> {abs_target_path} ({format_size(size)})")
            shutil.move(str(abs_source_path), str(abs_target_path))

            # Update state to "done"
            current_state_files[hash_val] = "done"
            if state_file_path:
                operations.update_operation_state_file(state_file_path, hash_val, "done")

            moved_count += 1
            total_bytes_moved += size

        except SymbolicPathError as e:
            error_msg = f"failed: Path resolution error: {e}"
            logger.error(f"{progress} Failed to resolve path for {source_symbolic_path} or target: {e}")
            current_state_files[hash_val] = error_msg
            failed_count += 1
            if state_file_path:
                operations.update_operation_state_file(state_file_path, hash_val, error_msg)
        except OSError as e:
            logger.error(f"{progress} Failed to move {source_symbolic_path}: {e}")
            error_msg = f"failed: {type(e).__name__}: {e}"
            # Check if source exists - maybe moved previously but state update failed?
            if not abs_source_path.exists() and abs_target_path.exists() and abs_target_path.stat().st_size == size:
                 logger.warning(f"  -> Source missing, target exists with correct size. Marking as done.")
                 current_state_files[hash_val] = "done" # Assume it was moved successfully before crash
                 moved_count += 1 # Count it as moved for stats
                 total_bytes_moved += size
                 if state_file_path:
                    operations.update_operation_state_file(state_file_path, hash_val, "done")
            else:
                 current_state_files[hash_val] = error_msg
                 failed_count += 1
                 if state_file_path:
                    operations.update_operation_state_file(state_file_path, hash_val, error_msg)

        except Exception as e: # Catch any other unexpected error
            error_msg = f"failed: Unexpected error: {type(e).__name__}: {e}"
            logger.exception(f"{progress} An unexpected error occurred moving {source_symbolic_path}: {e}")
            current_state_files[hash_val] = error_msg
            failed_count += 1
            if state_file_path:
                operations.update_operation_state_file(state_file_path, hash_val, error_msg)


    # --- 5. Cleanup ---
    logger.info("Move process finished.")
    if state_file_path:
        # Reload state to get final counts before deciding to delete
        final_state = operations.load_operation_state(state_file_path)
        final_failed_count = 0
        if final_state:
            final_failed_count = sum(1 for status in final_state['files'].values() if isinstance(status, str) and status.startswith("failed"))
        else:
            logger.warning(f"Could not reload final state from {state_file_path} before cleanup.")
            # Assume failure if we can't reload the state
            final_failed_count = failed_count if failed_count > 0 else 1 # If initial failed>0 use that, else assume 1 failure

        if final_failed_count == 0:
            logger.info("All files moved successfully. Deleting operation state file.")
            operations.delete_operation_state(state_file_path)
        else:
            logger.warning(f"{final_failed_count} file(s) failed to move. "
                           f"State file kept at: {state_file_path}. Use 'resume' command to retry.")
            failed_count = final_failed_count # Update failed count based on final state

    # --- 6. Return Stats ---
    stats = {
        "moved_files": moved_count,
        "failed_files": failed_count,
        "skipped_files": skipped_count, # Relevant only for dry run
        "total_bytes_moved": total_bytes_moved,
        "target_size_gb": size_limit_gb, # The requested limit
        "actual_moved_size_bytes": total_bytes_moved,
    }
    logger.info(f"Move Summary: Moved={moved_count}, Failed={failed_count}, Skipped={skipped_count}, "
                f"Bytes Moved={format_size(total_bytes_moved)}")
    return stats 