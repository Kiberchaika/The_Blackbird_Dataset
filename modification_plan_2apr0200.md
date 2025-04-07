Okay, let's break down the modifications needed to introduce multiple storage locations to the Blackbird Dataset Manager. The goal is to integrate this feature seamlessly while minimizing changes to the existing codebase, especially the public API, and ensuring robustness and testability.

**Core Idea:** We will introduce a `locations.json` file to define named paths. The index (`index.pickle`) will store file paths *symbolically*, prepending the location name (e.g., `Main/Artist/Album/track.mp3` or `SSD_Fast/Artist/Album/track.mp3`). A central path resolution mechanism will translate these symbolic paths to actual absolute paths on disk when needed.

---

**Phase 1: Configuration and Location Management**

**Goal:** Introduce the `locations.json` file and basic management via code and CLI.

**1.1. Define `locations.json` Structure and Default:**

*   **File:** `.blackbird/locations.json`
*   **Structure:** A JSON object mapping location names (strings) to absolute paths (strings).
    ```json
    {
      "Main": "/path/to/dataset/root",
      "LocationName2": "/path/to/other/storage"
    }
    ```
*   **Default:** If the file doesn't exist, assume a single location named "Main" pointing to the dataset root path (`dataset.path`).
*   **Code:**
    *   Create a helper class, potentially `blackbird.locations.LocationsManager`, initialized with the dataset root path.
    *   Add methods like `load_locations()`, `save_locations()`, `get_location_path(name)`, `get_all_locations()`, `add_location(name, path)`, `remove_location(name)`.
    *   `load_locations()` should handle the default case gracefully.
    *   Add validation in `add_location` (path exists, is dir, name unique) and `remove_location` (cannot remove "Main" if it's the only one or if target location for move exists).
    *   Integrate `LocationsManager` into the `Dataset` class: `self.locations = LocationsManager(self.path)`. Load locations upon initialization.
    
    **Result:**
    *   Added `blackbird/locations.py`: Contains `LocationsManager` class for handling multiple storage locations.
    *   Modified `blackbird/dataset.py`: Integrated `LocationsManager` into the `Dataset` class.

    **Implemented**
 
**1.2. Implement CLI Commands for Location Management:**

*   **Code (`cli.py`):**
    *   Add a new command group `location` under `main`.
    *   Add `blackbird location list`: Loads `locations.json` and prints names and paths.
    *   Add `blackbird location add <name> <path>`: Uses `LocationsManager.add_location()` and saves. Validates path existence.
    *   Add `blackbird location remove <name>`: Uses `LocationsManager.remove_location()` and saves. Confirms with the user, especially if data might exist there (warn them). Cannot remove "Main" if it's the only location.
*   **Refactor (`cli.py`):** Ensure other commands that take `dataset_path` can load the `LocationsManager`.
    
    **Result:**
    *   Modified `blackbird/cli.py`: Added `location` command group with `list`, `add`, `remove` subcommands utilizing `LocationsManager`.

    **Implemented**

**1.3. Tests:**

*   **New Test File (`test_locations.py`):**
    *   Test `LocationsManager` loading default location when `locations.json` is absent.
    *   Test loading/saving `locations.json` with multiple entries.
    *   Test `add_location` (success, duplicate name error, invalid path error).
    *   Test `remove_location` (success, removing non-existent, trying to remove "Main" inappropriately).
    *   Test `get_location_path`.
*   **New Test File (`test_cli_locations.py`):**
    *   Use `CliRunner` to test `blackbird location list`, `add`, `remove` commands.
    *   Mock the `LocationsManager` methods or test against a temporary `locations.json`.
    

---

**Phase 2: Indexing with Symbolic Paths**

**Goal:** Modify indexing to scan all locations and store paths symbolically, without changing the `DatasetIndex.build` signature. Implement path resolution.

**2.1. Modify `DatasetIndex.build` Internal Logic:**

*   **Code (`index.py`):**
    *   Inside `build(cls, dataset_path: Path, schema: 'DatasetComponentSchema', ...)`:
        *   Load locations using `LocationsManager(dataset_path).load_locations()`.
        *   Get the list of absolute root paths: `[locations['Main'], locations['Location2'], ...]`.
        *   Modify the `os.walk` loop to iterate through *all* root paths.
        *   When calculating `rel_path`, ensure it's relative to the *specific root path* it was found under.
        *   **Crucially:** Before storing in `TrackInfo.files` and `TrackInfo.file_sizes`, prepend the location name: `symbolic_path = f"{location_name}/{rel_path}"`. Store this `symbolic_path`.
        *   The `track_path` key in `index.tracks` should also become symbolic: `symbolic_track_path = f"{location_name}/{artist}/{album}/.../{base_name}`. Adjust logic for `track_by_album` and `album_by_artist` accordingly (store symbolic track/album paths).
        *   When getting file size (`file_path.stat().st_size`), use the *absolute* path derived *before* creating the symbolic path.

    **Result:**
    *   Modified `blackbird/index.py`: Updated the `DatasetIndex.build` method to scan all configured locations, calculate relative paths within each location, and store symbolic paths (e.g., `LocationName/Artist/Album/Track_Component.ext`) in the index structures (`tracks`, `track_by_album`, `album_by_artist`, `TrackInfo.files`, `TrackInfo.file_sizes`).

    **Implemented**

**2.2. Implement Path Resolution:**

*   **Code (`locations.py` or `utils.py`):
    *   Create `resolve_symbolic_path(symbolic_path: str, locations: Dict[str, str]) -> Path`:
        *   Parses `symbolic_path` like "LocationName/rest/of/path".
        *   Looks up `locations[LocationName]`.
        *   Returns `Path(locations[LocationName]) / "rest/of/path"`.
        *   Handle potential errors (invalid format, unknown location name).
*   **Code (`Dataset` class):** Add a method `resolve_path(symbolic_path: str) -> Path` that uses its `self.locations` and the utility function.

    **Result:**
    *   Added `resolve_symbolic_path` function and `SymbolicPathError` to `blackbird/locations.py`.
    *   Added `resolve_path` method to `Dataset` class in `blackbird/dataset.py`.

    **Implemented**

**2.3. Adapt Index Statistics:**

*   **Code (`index.py`):**
    *   Modify `build` to calculate per-location stats (track/artist/album counts, size). Store these within the `DatasetIndex` object (e.g., `index.stats_by_location = {'Main': {...}, 'SSD_Fast': {...}}`).

    **Result:**
    *   Added `stats_by_location` field to `DatasetIndex` dataclass in `blackbird/index.py`.
    *   Updated `DatasetIndex.build` method to populate `stats_by_location` with file, size, track, album, and artist counts per location.

    **Implemented**

**2.4. Tests:**

*   **Modify `test_index.py`:**
    *   Update fixtures (`sample_index`) to use symbolic paths (`Main/Artist1/...`).
    *   Ensure search tests (`test_search_by_artist`, `test_search_by_album`, `test_search_by_track`) still work transparently with symbolic paths. Add assertions that results span multiple locations if the fixture includes them.
*   **New Tests (`test_index.py` or `test_locations.py`):**
    *   Test `resolve_symbolic_path` utility function with various valid and invalid inputs.
    *   Test `Dataset.resolve_path` method.
*   **Modify `test_real_dataset_index.py`:**
    *   Ensure it runs correctly, loading the potentially existing `locations.json`.
    *   Verify the index contains symbolic paths.
    *   Verify per-location stats are generated.
    
    **Implemented**

    **Result:**
    *   Modified `blackbird/tests/test_index.py`: Updated fixtures to use symbolic paths, added assertions to ensure search works across locations, and added a test for `stats_by_location`.
    *   Modified `blackbird/tests/test_locations.py`: Added tests for the `resolve_symbolic_path` utility function using real temporary paths.
    *   Modified `blackbird/tests/test_dataset.py`: Added tests for the `Dataset.resolve_path` method and updated existing tests to correctly handle symbolic paths and multi-location datasets.

---

**Phase 3: Adapt Core Functionality**

**Goal:** Ensure existing search, stats, and sync operations work correctly with symbolic paths and multiple locations.

**3.1. Adapt Search and Analysis:**

*   **Code (`dataset.py`):**
    *   Review `find_tracks` and `analyze`. Ensure they rely on the `DatasetIndex` search methods which should now handle symbolic paths transparently. No changes should be needed if Phase 2 was done correctly.
    *   Where absolute paths are needed (e.g., returning `List[Path]` in `find_tracks`), use `self.resolve_path()`.
*   **Code (`cli.py`):
    *   Modify `stats` command: If it reads the index directly, ensure it handles symbolic paths. Better: rely on `Dataset.analyze` or stats stored in the index. Display per-location stats.
    *   Modify `find-tracks` command: Ensure it uses `Dataset.find_tracks` and correctly displays resolved paths or symbolic paths as appropriate.
    *   Modify `reindex` command: Ensure it correctly triggers the updated `DatasetIndex.build`.
    *   Modify `schema show`: If it accesses file paths, use the resolver.

**Result:**
*   Modified `blackbird/cli.py`: Updated `stats` command to display per-location statistics loaded from the index.
*   Validated via CLI commands (`reindex`, `stats`, `find-tracks` with multi-location setup) that search and analysis functions correctly handle symbolic paths and multiple locations after the `stats` command update. Initial validation failures were due to using incorrect artist names for testing.

**Implemented**

**3.2. Adapt Sync/Clone (`sync.py`):**

*   **Code (`sync.py` - `DatasetSync.sync`, `clone_dataset`):**
    *   When determining `files_to_sync`, the keys will be symbolic paths from the *remote* index.
    *   Introduce `target_location_name: Optional[str] = "Main"` argument to `sync`.
    *   Introduce `target_location: Optional[str] = "Main"` argument to `clone_dataset`.
    *   Inside the download loop (`_download_file` or main loop) within `sync`:
        *   Use `self.dataset.locations` to resolve the *local* absolute path based on the `target_location_name` and the *relative* part of the symbolic path.
        *   Strip the location prefix from the symbolic path to get the `relative_path` to pass to the `WebDAVClient`'s `download_file` method as the remote path.
        *   Local file existence check (`local_file.exists()`) must use the resolved absolute path for the target location.
*   **Code (`cli.py`):
    *   Add `--target-location <name>` option to `sync` and `clone` commands, passing it down.

**Result:**
*   Modified `blackbird/cli.py`: Added `--target-location` option to `clone` and `sync` commands.
*   Modified `blackbird/sync.py`: Updated `clone_dataset` and `DatasetSync.sync` signatures. Modified `sync` method to validate `target_location_name`, resolve local paths to the target location, and strip location prefixes from remote paths before download. Updated resume logic to check paths in the target location.

**Implemented**

**3.3. Enhance CLI Status (`cli.py`):**

*   **Code (`cli.py` - `main` function or a new `status` command):**
    *   If invoked within a dataset directory:
        *   Load `LocationsManager` and `DatasetIndex`.
        *   Print locations from `locations.json`.
        *   Print last index time from `index.last_updated`.
        *   Print per-location stats stored in the index during Phase 2.3.

**Result:**
*   Modified `blackbird/cli.py`: Added logic to the `main` command group to detect when run without subcommands in a dataset directory. It now prints dataset status including locations, index last updated time, and per-location statistics.
*   Added `blackbird/utils.py`: Created this file and added a `format_size` utility function.

**Implemented**

**3.4. Tests:**

*   **Modify `test_dataset.py`:**
    *   Update fixtures to potentially span multiple (mocked) locations.
    *   Ensure `test_find_tracks_*` tests still pass, potentially adding assertions that results include files resolved to different locations.
    *   Verify `test_analyze_dataset` reports aggregate stats correctly.
*   **Modify `test_sync_command.py`, `test_selective_sync.py`, `test_webdav_sync.py`:**
    *   Update fixtures to mock `locations.json`.
    *   Test syncing *to* a specific non-"Main" location using the `--target-location` CLI option. Verify files land in the correct absolute path.
    *   Test that `clone` works correctly, defaulting to "Main".
    *   Ensure sync correctly identifies existing files using resolved paths even if they are in different locations.
*   **New Tests (`test_cli_status.py`):**
    *   Test the default status output when running `blackbird` inside a dataset dir. Mock the index and locations file.
    
    **Implemented**
    
    **Result:**
    *   Added `blackbird/tests/test_cli_status.py`: Contains tests for the CLI status command covering normal operation, running outside a dataset, missing locations file, and missing index file (verified automatic rebuild behavior).

---

**Phase 4: Interruption Handling and Minimal Hashes**

**Goal:** Implement saving/resuming for sync and move operations using minimal hashes stored in the index.

**4.1. Minimal Hashing:**

*   **Concept:** Use a fast, non-cryptographic hash (like `xxhash` if dependency is acceptable, or Python's `hash()`) of the *symbolic path string*. This avoids reading file content.
*   **Code (`index.py`):**
    *   Add a field `symbolic_path_hash: int` to `TrackInfo`.
    *   Modify `DatasetIndex.build`: Calculate `hash(symbolic_path)` and store it.
    *   Add a lookup dictionary `index.tracks_by_hash: Dict[int, str]` mapping hash to symbolic track path.
    *   Add a method `index.get_track_by_hash(hash_val)`? Or maybe `get_files_by_hash` returning a list of symbolic file paths associated with that track hash. Decide on the granularity. Hashing *each file's symbolic path* might be more robust for resuming specific file transfers. Let's hash *symbolic file paths*.
    *   Modify `DatasetIndex.build`: Create `index.file_info_by_hash: Dict[int, Tuple[str, int]]` mapping `hash(symbolic_file_path)` to `(symbolic_file_path, size)`.
    *   Add `index.get_file_info_by_hash(hash_val)`.

    **Result:**
    *   Modified `blackbird/index.py`: Added `file_info_by_hash` field to `DatasetIndex`, added `get_file_info_by_hash` method, and updated `build` method to calculate and store hashes of symbolic file paths.
    
    **Implemented**

**4.2. Operation State File:**

*   **Structure:** JSON file, e.g., `.blackbird/operation_sync_1678886400.json`.
    ```json
    {
      "operation_type": "sync", // or "move"
      "timestamp": 1678886400.123,
      "source": "webdav://...", // or source location name for move
      "target_location": "Main", // or target location name
      "components": ["vocals", "mir"], // if applicable
      "files": {
         "hash1": "pending",
         "hash2": "done",
         "hash3": "failed: Error message"
         // ... using minimal file hashes as keys
      }
    }
    ```
*   **Code (`sync.py`, New `mover.py`?):**
    *   Before starting sync/move, create the state file, listing all file hashes as "pending".
    *   During the operation, periodically (e.g., after each file or batch) update the status (`done` or `failed`) in the state file.
    *   On successful completion, delete the state file.

    **Result:**
    *   Added `blackbird/operations.py`: Contains functions for creating, loading, updating, and deleting operation state files.
    *   Modified `blackbird/sync.py`: Integrated calls to `operations.py` functions within the `DatasetSync.sync` method to manage the lifecycle of state files during synchronization.

    **Implemented**

**4.3. Resume Logic:**

*   **Code (`cli.py`):** Add `blackbird resume <operation_file.json>` command.
*   **Code (New `resumer.py` or within `sync.py`/`mover.py`):**
    *   Load the operation state file.
    *   Load the *current* index (potentially re-indexing the target location(s) first might be needed for accuracy, or rely on file existence checks).
    *   Iterate through "pending" and "failed" hashes in the state file.
    *   Use `index.get_file_info_by_hash(hash)` to get the symbolic path and expected size.
    *   Check if the file *already exists* at the target location with the correct size (using the path resolver). If yes, mark as "done" in the state file and skip.
    *   If not, attempt the download/move again.
    *   Update the state file periodically.
    *   Delete state file on completion.
    
    **Result:**
    *   Modified `blackbird/cli.py`: Added the `resume` command.
    *   Modified `blackbird/sync.py`: Implemented `resume_sync_operation` function containing the core logic for resuming sync operations based on the state file.
    
    **Implemented**

**4.4. Tests:**

*   **Modify `test_index.py`:** Test hash generation and lookup (`file_info_by_hash`).
*   **New Test File (`test_resume.py`):**
    *   Test state file creation and deletion.
    *   Test sync interruption: Mock a download failure, run sync, verify state file exists with pending/failed items. Run `resume`, verify remaining files are processed, and state file is deleted. Use mocks for `WebDAVClient`.
    *   Test move interruption (similar logic, mocking `shutil.move` failure).
    *   Test resuming with some files already correctly present in the target location.

    **Result:**
    *   Modified `blackbird/tests/test_index.py`: Added tests for hash generation and lookup.
    *   Added `blackbird/tests/test_resume.py`: Implemented comprehensive tests for state file handling and sync resume logic, including success, failure, CLI integration, path inference, and handling existing/mismatched files. (Move resume tests are pending move implementation).

    **Implemented**

---

**Phase 5: Data Balancing and Moving**

**Goal:** Implement logic and CLI commands for moving data between locations.

**5.1. Move Logic (Code - New `mover.py` or `locations.py`):**

*   Create `move_data(source_location: str, target_location: str, size_limit_gb: Optional[float] = None, specific_folders: Optional[List[str]] = None, dataset: Dataset)` function/method.
*   Identify Files:
    *   Load index.
    *   Filter tracks/files belonging to `source_location`.
    *   If `specific_folders` provided, filter further (match symbolic paths like `SourceLoc/Artist/Album`).
    *   If `size_limit_gb` provided, iterate through files, summing size until the limit is reached (consider moving whole albums/artists for coherence if not `specific_folders`).
*   Use Interruption Handling: Integrate with the state file mechanism from Phase 4. List source/target symbolic paths using hashes.
*   Perform Move:
    *   Iterate through the list of files to move.
    *   For each file hash:
        *   Get source symbolic path and target symbolic path (replace location name).
        *   Resolve absolute source and target paths using `dataset.resolve_path`.
        *   Ensure target directory exists.
        *   Use `shutil.move(abs_source_path, abs_target_path)`.
        *   Update state file entry to "done". Handle errors and mark as "failed".
*   Re-index: After successful completion (state file deleted), trigger `dataset.rebuild_index()`.

**Implemented**

**Result:**
*   Added `blackbird/mover.py`: Contains `move_data` function for identifying, moving, and tracking files between locations with interruption/resume support via state files.

**5.2. CLI Commands (`cli.py`):**

*   Add `blackbird location balance <dataset_path> <source_loc> <target_loc> --size <N>`:
    *   Confirms with the user ("Are you sure you want to move data...?").
    *   Calls the `move_data` function.
    *   Includes `--dry-run` option.
    *   Reports progress and stats.
    *   Triggers reindex on success.
*   Add `blackbird location move-folders <dataset_path> <target_loc> FOLDER [FOLDER ...] --source-location <source_loc>`:
    *   Confirms with the user.
    *   Calls `move_data` with `specific_folders`.
    *   Includes `--dry-run` option.
    *   Reports progress and stats.
    *   Triggers reindex on success.

**Implemented**

**Result:**
*   Modified `blackbird/cli.py`: Added `location balance` and `location move-folders` subcommands, utilizing the `move_data` function and including confirmation, dry-run, progress reporting, and automatic re-indexing upon completion.

**5.3. Tests:**

*   **New Test File (`test_mover.py`):
    *   Test identifying files for moving based on size limit.
    *   Test identifying files based on specific folders.
    *   Test the `shutil.move` logic with path resolution.
    *   Integrate tests with interruption/resume logic from Phase 4.
    *   Test edge cases (moving 0 GB, moving more than available, invalid locations).
*   **New Test File (`test_cli_move.py`):
    *   Test `balance` and `move-folders` commands using `CliRunner`. Mock the actual move logic but verify parameters and confirmation.

**Implemented**

**Result:**
*   Added `blackbird/tests/test_mover.py`: Contains unit tests for the core `move_data` logic.
*   Added `blackbird/tests/test_cli_move.py`: Contains integration tests for the CLI `location balance` and `location move-folders` commands.

---

**Final Review and Refinement:**

*   Ensure consistent use of the path resolver (`resolve_symbolic_path`).
*   Verify error handling and user feedback (confirmations, progress bars).
*   Check for potential race conditions if multi-threading is used (esp. state file updates).
*   Update documentation (`README.md`, potentially `blackbird_dataset_full_spec.md`).

This plan breaks down the complex requirements into manageable phases, prioritizing core changes first and building upon them. The test-driven approach ensures functionality is verified at each step. Remember to handle edge cases like empty locations, non-existent paths, and concurrent operations if applicable.

Whenever you add some locations inside unit tests, use these specific ones:
/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_2
/home/k4/Projects/The_Blackbird_Dataset/test_dataset_folder_3
They have some real data to test with. Do not delete them or their content.