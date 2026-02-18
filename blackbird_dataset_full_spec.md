# Blackbird Dataset Manager Specification

## Overview

Blackbird Dataset Manager is a Python package designed to manage and synchronize music datasets with multiple components. It's specifically built to handle datasets where each track can have multiple associated files (instrumentals, vocals, MIR data, etc.) while maintaining a clear and consistent structure.

It supports distributing the dataset across multiple storage locations, configured via a `.blackbird/locations.json` file.

It also provides a **Streaming Pipeline** (`StreamingPipeline`) for processing remote datasets without downloading the entire dataset upfront: files are downloaded into a bounded queue, the user processes them, results are uploaded back to the WebDAV server, and local files are cleaned up automatically.

Here's an example of different files (we call them components) for a single track in the dataset:
```
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_instrumental.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix).mir.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_noreverb.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_noreverb.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section10.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section10.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section12.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section12.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section13.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section13.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section1.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section1.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section3.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section3.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section4.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section4.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section5.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section5.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section6.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section6.mp3'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section8.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section8.mp3'
'11.Юта - Жili-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section9.json'
'11.Юта - Жili-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section9.mp3'
```

## Core Concepts

### 1. Dataset Structure and Locations

The dataset follows a fixed hierarchical structure within each configured storage location:
```
location_root/ # e.g., /mnt/hdd/dataset or /mnt/ssd/dataset_part
├── Artist1/
│   ├── Album1/
│   │   ├── track1_instrumental.mp3
│   │   ├── track1_vocals_noreverb.mp3
│   │   └── ...
│   └── Album2/
│       ├── CD1/        # Optional CD-level for multi-CD albums
│       │   ├── track1_instrumental.mp3
│   │   └── ...
│   └── CD2/
│       ├── track1_instrumental.mp3
│       └── ...
└── ...
```

Multiple storage locations can be defined in `.blackbird/locations.json` within the primary dataset directory (the one containing `.blackbird`).
```json
{
  "Main": "/path/to/primary/storage",
  "SSD_Fast": "/path/to/secondary/storage"
}
```
If this file doesn't exist, a single location named "Main" pointing to the dataset root directory is assumed.

The directory structure within each location is fixed and non-configurable:
1. Artist level (required)
2. Album level (required)
3. CD level (optional, must match pattern `CD\d+`)
4. Track files with component-specific suffixes

**Symbolic Paths:** The dataset index (`index.pickle`) stores file paths *symbolically*, prepending the location name (e.g., `Main/Artist1/Album1/track1_instrumental.mp3`, `SSD_Fast/Artist2/Album3/track2_vocals.mp3`). A path resolution mechanism translates these symbolic paths to actual absolute paths on disk when needed.

### 2. Schema Management

The schema system defines ONLY the types of files (components) that can exist for each track. It does NOT define the directory structure, which is fixed. The schema specifies:
1. What component types exist (e.g., instrumentals, vocals, MIR data)
2. The naming pattern for each component type (including file extensions)
3. Whether multiple files of a component are allowed per track
4. Human-readable descriptions of each component's purpose

When syncing from a remote source, if the local schema does not have a component that exists remotely and was requested, the selective sync operation automatically updates the local schema. This ensures that different machines can maintain different subsets of components based on their needs and that tracking is handled by the schema file. Also, different components can be available for different number of tracks, e.g. some tracks might have a sectioned lyrics component, while others might not.

### 2.1 Schema Discovery

The schema discovery process automatically analyzes the dataset structure to generate a schema that matches the existing files. This is done through the `discover_schema` method which:

1. **Component Detection**
   - Component file naming pattern is the file extension plus everything that precedes it if there's any amount of characters that start with _ in the end of the file name before the extension.
   E.g. `track1_vocals_stretched_120bpm.mp3` has the component pattern `vocals_stretched_120bpm.mp3`
   - A special case of component is a multiple file component that has a number right before the file extension.
   E.g. `track1_vocals_stretched_120bpm_section1.mp3` has the component pattern `vocals_stretched_120bpm_section*.mp3`
   - Component name/id can be descriptive, but the component pattern is used to find the actual files
   - The default Component name is the component pattern with the file extension, but shortened if it's especially long


Example discovery usage:
```python
# Using the utility script
./utils/discover_and_save_schema.py /path/to/dataset --num-artists 25

# Or programmatically
schema = DatasetComponentSchema(dataset_path)
schema.discover_schema(folders=["Artist/Album"])  # Optional folder filter

# Access discovered components
for name, config in schema.schema["components"].items():
    print(f"Component: {name}")
    print(f"Pattern: {config['pattern']}")  # Includes exact file extension
    print(f"Multiple files allowed: {config['multiple']}")
    print(f"Description: {config['description']}")
    
    # Access statistics (discover_schema returns SchemaDiscoveryResult)
    result = schema.discover_schema(folders=["Artist/Album"])
    if name in result.stats["components"]:
        stats = result.stats["components"][name]
        print(f"Files found: {stats['file_count']}")
        print(f"Track coverage: {stats['track_coverage']*100:.1f}%")
```

#### Schema Design Rationale

1. **Component-Only Focus**
   - Schema ONLY defines file types and their patterns
   - Does NOT affect the fixed artist/album/[cd]/track structure
   - Allows different machines to work with different subsets of components

2. **Component Definitions**
   - `pattern`: Glob pattern for identifying component files (includes exact file extension)
   - `multiple`: Whether multiple files of this type are allowed per track
   - `description`: Human-readable description of the component's purpose

3. **File Extension Handling**
   - File extensions are treated as part of the component pattern
   - Extensions must match exactly (e.g., `.mp3` won't match `.MP3`)
   - Compound extensions are preserved (e.g., `.mir.json`, `.stretched.mp3`)
   - Extensions are used to determine component types (e.g., `_audio` suffix for `.mp3` files)

4. **Multiple Files Per Component**
   - Components can be configured to allow multiple files per base track
   - To enable multiple files, the following strict naming convention MUST be followed:
     - Files must have a number as the last part before the extension
     - Example: `track_vocals1.mp3`, `track_vocals2.mp3`, etc.
   - This is the ONLY supported format for components with multiple files
   - Numbers can be any length and indicate the sequence within the component
   - Files without this exact format will not be recognized as part of a multiple-file component

Example schema.json:
```json
{
  "version": "1.0",
  "components": {
    "vocals_audio": {
      "pattern": "*_vocals.mp3",
      "multiple": false,
      "description": "Acapella files"
    },
    "mir": {
      "pattern": "*.mir.json",
      "multiple": false,
      "description": "Music Information Retrieval files"
    },
    "vocals_noreverb_lyrics": {
      "pattern": "*_vocals_noreverb.json",
      "multiple": false,
      "description": "Lyrics from Whisper"
    },
    "instrumental_audio": {
      "pattern": "*_instrumental.mp3",
      "multiple": false,
      "description": "Instrumental file with vocals removed"
    },
    "vocals_stretched_audio": {
      "pattern": "*_vocals_stretched.mp3",
      "multiple": true,
      "description": "Vocals file stretched to 120bpm"
    },
    "vocals_stretched_lyrics_section": {
      "pattern": "*_vocals_stretched_lyrics_*section*.json",
      "multiple": true,
      "description": "A lyrics corresponding to the section of the vocals"
    },
    "caption": {
      "pattern": "*_caption.txt",
      "multiple": false,
      "description": "Caption from a human captioner describing voice"
    }
  }
}
```

#### Schema Validation

The schema validation process ensures:

1. **Pattern Uniqueness**
   - Each component pattern must uniquely identify its files
   - No pattern collisions between components
   - File extensions are part of uniqueness check

2. **Multiple Files Constraint**
   - Components with `multiple: false` must have exactly one file per track
   - Section-based components automatically get `multiple: true`

3. **Directory Structure**
   - Validates against fixed artist/album/[cd]/track structure
   - Ensures CD directories match pattern when present
   - Verifies maximum directory depth

Example validation:
```python
schema = DatasetComponentSchema(dataset_path)

# Validate schema structure and directory layout
result = schema.validate()

if result.is_valid:
    print("Schema validation successful")
    print(f"Artists: {result.stats['directory_structure']['artists']}")
    print(f"Albums: {result.stats['directory_structure']['albums']}")
else:
    print("Validation errors:")
    for error in result.errors:
        print(f"- {error}")

# Validate schema against actual data (checks file matching and constraints)
result = schema.validate_against_data()

if result.is_valid:
    print(f"Total files: {result.stats['total_files']}")
    print(f"Matched files: {result.stats['matched_files']}")
else:
    print("Data validation errors:")
    for error in result.errors:
        print(f"- {error}")
```

### 3. Synchronization

Blackbird uses WebDAV for dataset synchronization, with several key features:

#### 3.1 Pull-Only Sync Design
- Sync/clone operations only support pulling from remote to local
- Rationale: Safer than bi-directional sync, prevents accidental data modification
- Each machine maintains its own subset of components
- **Exception:** The `StreamingPipeline` (section 3.6) supports uploading processing results back to the server via `submit_result()`

#### 3.2 Parallel Synchronization
- Supports concurrent downloads with configurable parallelism
- Clone and sync operations use the same underlying sync mechanism
- Configurable connection pool size for optimized network performance
- Optional HTTP/2 support for improved transfer speeds

#### 3.3 Schema and Index Handling During Sync

When syncing from a remote source, the schema and index are handled automatically:

1. **Schema Handling**
   - If no local schema exists (`.blackbird/schema.json`):
     - A new schema is created automatically
     - Components from the remote schema that correspond to the files being pulled are added to the local schema
   - If local schema exists:
     - New components from the remote schema are merged into the local schema if they correspond to files being pulled
     - Existing components in the local schema are preserved
   - The schema is updated as part of the sync process, no manual schema management is needed

2. **Index Handling**
   - The remote index is always downloaded in full during sync operations
   - This ensures accurate file tracking regardless of which components are being synced
   - The index is automatically saved to `.blackbird/index.pickle`

Example sync process:
```python
from blackbird.dataset import Dataset
from blackbird.sync import DatasetSync, configure_client

dataset = Dataset("/path/to/local")
client = configure_client("webdav://192.168.1.100:8080/dataset")
sync = DatasetSync(dataset)
stats = sync.sync(client, components=["vocals_audio", "mir"], artists=["Artist1"])
```

This process ensures that:
- The local schema always reflects the components actually present in the local dataset
- New components are added automatically when their files are synced
- The index stays in sync with the remote dataset
- No manual schema or index management is required

#### 3.4 Selective Component Sync
```python
sync = DatasetSync(dataset)
stats = sync.sync(client, components=["vocals", "mir"])  # Only sync specific components
```
- Can sync specific components instead of entire dataset
- Uses remote schema patterns to find relevant files
- Maintains component consistency between datasets

#### 3.5 Progress Tracking
- Real-time progress updates using tqdm
- Shows file counts and byte totals
- Displays transfer speed and estimated time remaining
- Reports errors with detailed messages

#### 3.6 Streaming Pipeline (Python API)

The `StreamingPipeline` class (`blackbird/streaming.py`) enables processing a remote dataset in a streaming fashion: download files into a bounded queue, process them locally, upload results back to the same WebDAV server, and clean up local files. This avoids downloading the entire dataset upfront.

**Architecture:**
```
StreamingPipeline
├── Download workers (N threads) → download_queue (bounded, backpressure)
│     └── WebDAVClient.download_file() with 3x retry + backoff
├── User code: take() → process → submit_result()
├── Upload workers (M threads) ← upload_queue (non-blocking submit)
│     └── WebDAVClient.upload_file() with 3x retry + backoff
└── State file (.pipeline_state.json) for resume after crash/interrupt
```

**Key classes:**
- `StreamingPipeline` — main orchestrator, context manager
- `PipelineItem` — dataclass with `local_path`, `remote_path`, `metadata` (artist, album, track, component)

**Public methods:**

| Method | Description |
|--------|-------------|
| `take(count=1)` | Block until `count` downloaded files are ready. Returns `list[PipelineItem]` or `[]` when dataset is exhausted |
| `submit_result(item, result_path, remote_name)` | Queue result for background upload. After upload both local files (source + result) are deleted |
| `skip(item)` | Delete local source without uploading |

**Constructor parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | (required) | WebDAV server URL (`https://`, `http://`, or `webdav://` — automatically converted to `webdav://` internally) |
| `components` | `None` (all) | List of component types to download |
| `artists` | `None` (all) | Filter by artist names |
| `albums` | `None` (all) | Filter by album names |
| `queue_size` | `10` | Max files in the prefetch queue (backpressure) |
| `prefetch_workers` | `4` | Number of download threads |
| `upload_workers` | `2` | Number of upload threads |
| `work_dir` | `/tmp/blackbird_work` | Local working directory for downloads and results |
| `username` | `None` | WebDAV auth username |
| `password` | `None` | WebDAV auth password |

**Usage example:**
```python
from blackbird.streaming import StreamingPipeline

pipeline = StreamingPipeline(
    url="https://my-server.com/dataset",
    components=["original"],
    queue_size=8,
    prefetch_workers=4,
    upload_workers=2,
    work_dir="/tmp/blackbird_processing",
    username="user",
    password="pass",
)

with pipeline:
    while True:
        items = pipeline.take(count=4)
        if not items:
            break  # dataset fully processed

        for item in items:
            result = run_mir_analysis(item.local_path)
            save_path = item.local_path.with_suffix(".mir.json")
            save_json(result, save_path)

            pipeline.submit_result(
                item=item,
                result_path=save_path,
                remote_name=item.metadata["track"] + ".mir.json",
            )
```

**Resume support:** State is persisted to `work_dir/.pipeline_state.json`. On restart, already-processed files are skipped and pending uploads are retried.

**Error handling:**
- Download failures: retry 3x with exponential backoff, then skip + log
- Upload failures: retry 3x, file stays in `pending_uploads` state for next run
- Graceful shutdown (Ctrl+C): saves state, waits for in-flight uploads (30 sec timeout per upload worker thread)

**Upload support in WebDAVClient:** The `upload_file(local_path, remote_path)` method was added to `WebDAVClient` in `sync.py`. It uses HTTP PUT with automatic remote directory creation (MKCOL) and HTTP/2 support. Note: `upload_file()` itself is a single-attempt operation; retry logic is handled by `StreamingPipeline._upload_with_retry()`.


### 4. Track Management

Tracks are identified and managed through several mechanisms:

1. **Base Name Extraction**
   - Strips component suffixes to identify related files
   - Example: "track1_instrumental.mp3" → "track1"

2. **Track Path Generation**
   - Combines artist/album/[cd]/track for unique identification
   - Handles both regular and CD-based tracks

3. **Component Grouping**
   - Groups related files by their base track name
   - Validates component multiplicity rules

### 5. Dataset Analysis

Built-in analysis capabilities:
1. Component coverage statistics
2. Artist and album statistics
3. Track completeness analysis
4. Directory structure validation

## Components

### What is a Component?

A component in Blackbird represents a specific type of file that belongs to a track. Each track in the dataset can have multiple components, forming a complete set of related files. Components are identified by consistent naming patterns in their filenames.

Example track with multiple components:
```
track1_instrumental.mp3      # Instrumental component
track1_vocals_noreverb.mp3   # Vocals component
track1.mir.json              # MIR analysis component
track1_vocals_stretched_120bpm_section1.mp3  # Section component
```

### Component Names and Patterns

Each component in the schema consists of two key elements:
1. **Name** - An arbitrary identifier chosen by the user for organizational purposes. The name has no functional impact and serves only as a human-readable label for the component.
2. **Pattern** - The functional part that defines how files belonging to this component are identified. For example, `"*_vocals.mp3"` will match all files ending with `_vocals.mp3`.

Example schema components:
```json
{
  "components": {
    "my_vocals": {                        // Arbitrary name chosen by user
      "pattern": "*_vocals.mp3",          // Functional pattern that matches files
      "multiple": false,
      "description": "Any descriptive text"
    },
    "section_data": {                     // Another arbitrary name
      "pattern": "*_section*.json",       // Pattern is what matters
      "multiple": true,
      "description": "Any helpful note"
    }
  }
}
```

## Command Line Interface

Blackbird provides a comprehensive CLI for dataset operations after pip installation:

```bash
pip install blackbird-dataset
blackbird --help
```

When run inside a dataset directory without a subcommand, `blackbird` displays the current dataset status: configured locations, index statistics (tracks, artists, albums, component counts), and per-location breakdowns.

### 1. Dataset Cloning

```bash
# Clone entire dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local

# Clone specific components
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --components vocals,mir

# Clone only for tracks missing a specific component
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --components vocals,mir --missing caption

# Clone subset of artists (glob patterns supported)
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --artists "Artist1,Art*" --components vocals

# Clone proportion of dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --proportion 0.1 --offset 0

# Performance options
blackbird clone webdav://... /path/to/local \
    --parallel 4 --http2 --connection-pool 20 --profile \
    --target-location Main
```

**All clone options:**
| Option | Description |
|--------|-------------|
| `--components` | Comma-separated list of components to clone |
| `--missing` | Only clone for tracks missing this component |
| `--artists` | Comma-separated artist filter (glob patterns) |
| `--proportion` | Fraction of dataset to clone (0-1) |
| `--offset` | Offset for proportion-based cloning |
| `--parallel` | Number of parallel downloads (default: 1) |
| `--http2` | Use HTTP/2 for connections |
| `--connection-pool` | Connection pool size (default: 10) |
| `--target-location` | Location to clone into (default: Main) |
| `--profile` | Enable performance profiling |

### 2. Dataset Syncing

```bash
# Sync existing local dataset from remote
blackbird sync webdav://192.168.1.100:8080/dataset /path/to/local \
    --components vocals,mir --artists "Artist1"

# Sync with album filter
blackbird sync webdav://... /path/to/local \
    --artists "Artist1" --albums "Album1,Album2"

# Force reindex before syncing
blackbird sync webdav://... /path/to/local --force-reindex

# Debug mode
blackbird sync webdav://... /path/to/local --debug
```

**Additional sync options** (beyond clone options): `--albums`, `--force-reindex`, `--debug`, `--target-location`.

### 3. Resume Interrupted Operations

```bash
# Resume from a state file (required argument)
blackbird resume /path/to/.blackbird/operation_sync_1234567890.json

# Specify dataset path if it cannot be inferred
blackbird resume /path/to/state_file.json --dataset-path /path/to/dataset

# With performance options
blackbird resume /path/to/state_file.json --parallel 4 --http2 --profile --debug
```

### 4. Dataset Analysis

```bash
# Show dataset statistics (also accepts remote WebDAV URLs)
blackbird stats /path/to/dataset
blackbird stats webdav://192.168.1.100:8080/dataset

# Show statistics for tracks missing a specific component
blackbird stats /path/to/dataset --missing vocals

# Find tracks by component presence
blackbird find-tracks /path/to/dataset --missing vocals
blackbird find-tracks /path/to/dataset --has instrumental --missing caption
blackbird find-tracks /path/to/dataset --artist "Artist1" --album "Album1"

# Rebuild dataset index
blackbird reindex /path/to/dataset
```

### 5. Schema Management

```bash
# Show current schema (also accepts remote WebDAV URLs)
blackbird schema show /path/to/dataset
blackbird schema show webdav://192.168.1.100:8080/dataset

# Discover and save schema automatically
blackbird schema discover /path/to/dataset [--num-artists N] [--test-run]

# Add new component (NAME and PATTERN are positional arguments)
blackbird schema add /path/to/dataset lyrics "*.lyrics.json"
# Use --multiple flag if multiple files per track are allowed
blackbird schema add /path/to/dataset section_audio "*_section*.mp3" --multiple
```

### 6. Location Management

```bash
# List configured storage locations
blackbird location list /path/to/dataset

# Add a new storage location
blackbird location add /path/to/dataset SSD_Fast /mnt/fast_storage/dataset_part

# Remove a location (prompts for confirmation; use --yes to skip)
blackbird location remove /path/to/dataset SSD_Fast

# Move specific folders between locations (comma-separated paths in single arg)
blackbird location move-folders /path/to/dataset Main \
    "Artist1/Album1,Artist2/Album B/CD1" --source-location SSD_Fast

# Balance storage: move approx. 100GB from Main to SSD_Fast
blackbird location balance /path/to/dataset Main SSD_Fast --size 100 [--dry-run]
```

The dataset index stores file paths symbolically, prepending the location name (e.g., `Main/Artist/...`, `SSD_Fast/Artist/...`).

## WebDAV Server Setup

Blackbird includes a wizard for setting up a WebDAV server on Ubuntu using nginx:

```bash
# Start WebDAV setup wizard (--port is required)
blackbird webdav setup /path/to/dataset --port 8080
```

### Setup Process

1. **Dependency Check**
   ```bash
   Checking system requirements...
   Installing nginx and nginx-dav-ext-module...
   ```

2. **Nginx Configuration**
   ```bash
   Configuring nginx WebDAV module...
   Setting up authentication...
   Configuring dataset directory...
   ```

3. **Security Setup**
   ```bash
   Creating user credentials...
   Setting directory permissions...
   Configuring SSL (optional)...
   ```

4. **Testing**
   ```bash
   Starting nginx service...
   Testing WebDAV connection...
   Verifying file access...
   ```

### Server Configuration

The wizard creates a secure WebDAV configuration:

```nginx
# /etc/nginx/sites-available/blackbird-webdav.conf
server {
    listen 80;
    server_name _;  # Replace with your domain if needed

    # SSL configuration (optional)
    # listen 443 ssl;
    # ssl_certificate /etc/nginx/ssl/server.crt;
    # ssl_certificate_key /etc/nginx/ssl/server.key;

    root /path/to/dataset;
    
    location / {
        # WebDAV configuration
        dav_methods PUT DELETE MKCOL COPY MOVE;
        dav_ext_methods PROPFIND OPTIONS;
        
        # Read-only access (no PUT, DELETE, etc.)
        limit_except GET PROPFIND OPTIONS {
            deny all;
        }
        
        # Basic authentication
        auth_basic "Blackbird Dataset";
        auth_basic_user_file /etc/nginx/webdav.passwords;
        
        # Directory listing
        autoindex on;
        
        # Client body size (adjust as needed)
        client_max_body_size 0;
        
        # WebDAV performance tuning
        create_full_put_path on;
        dav_access user:rw group:r all:r;
    }
    
    # Access and error logs
    access_log /var/log/nginx/webdav.access.log;
    error_log /var/log/nginx/webdav.error.log;
}
```

### Security Features

1. **Authentication**
   - Basic auth with secure password storage
   - Optional SSL/TLS encryption
   - IP-based access control

2. **Permissions**
   - Read-only access by default
   - Separate user for WebDAV service
   - Proper file ownership

3. **Monitoring**
   - Access logging
   - Error logging
   - Bandwidth monitoring

### Testing

The wizard performs automatic testing:

1. **Connection Test**
   ```python
   from blackbird.sync import configure_client
   client = configure_client("webdav://localhost:8080")
   assert client.check_connection()
   ```

2. **File Access Test**
   ```python
   # Verify file download
   from pathlib import Path
   assert client.download_file("Artist/Album/track.mp3", Path("test.tmp"))
   ```

### CLI Implementation

The CLI is implemented using Click. The entry point is `main` (not `cli`), with `invoke_without_command=True` so running `blackbird` in a dataset directory shows status. Cloning uses the standalone `clone_dataset()` function from `sync.py`, syncing uses `DatasetSync.sync()`.

```python
from blackbird.sync import clone_dataset, DatasetSync, configure_client
from blackbird.dataset import Dataset

# Clone (standalone function)
stats = clone_dataset(
    source_url="webdav://server/dataset",
    destination=Path("/path/to/local"),
    components=["vocals", "mir"],
    artists=["Artist1"],
    parallel=4,
    use_http2=True,
)

# Sync (uses DatasetSync class)
dataset = Dataset("/path/to/local")
client = configure_client("webdav://server/dataset")
sync = DatasetSync(dataset)
stats = sync.sync(client, components=["vocals", "mir"])
```

## Dataset Indexing

Blackbird maintains a lightweight, fast index of the dataset for efficient operations. The index is stored in `.blackbird/index.pickle` using Python's pickle format with protocol 5 for optimal performance.

### Index Structure (with Symbolic Paths)

```python
@dataclass
class TrackInfo:
    """Track information in the index."""
    track_path: str      # Symbolic path identifying the track (Location/Artist/Album/[CD]/BaseName)
    artist: str         # Artist name
    album_path: str     # Symbolic path to album (Location/Artist/Album)
    cd_number: Optional[str]  # CD number if present
    base_name: str      # Track name without component suffixes
    files: Dict[str, str]  # component_name -> symbolic_file_path mapping (Location/Artist/.../file.ext)
    file_sizes: Dict[str, int]  # symbolic_file_path -> size in bytes

@dataclass
class DatasetIndex:
    """Main index structure."""
    last_updated: datetime
    tracks: Dict[str, TrackInfo]  # symbolic_track_path -> TrackInfo
    track_by_album: Dict[str, Set[str]]  # symbolic_album_path -> set of symbolic_track_paths
    album_by_artist: Dict[str, Set[str]]  # artist_name -> set of symbolic_album_paths
    total_size: int  # Total size of all indexed files
    total_files: int = 0  # Total number of indexed files
    stats_by_location: Dict[str, Dict] = field(default_factory=dict) # location_name -> {file_count, total_size, track_count, album_count, artist_count}
    file_info_by_hash: Dict[int, Tuple[str, int]] = field(default_factory=dict) # hash(symbolic_file_path) -> (symbolic_file_path, size)
    version: str = "1.0"
```

The index provides efficient access to:
1. Track information by its unique symbolic path.
2. All tracks (symbolic paths) in an album (identified by its symbolic path).
3. All albums (symbolic paths) by an artist.
4. File sizes for verification during sync (keyed by symbolic file path).
5. Component files (symbolic paths) for each track.
6. Per-location statistics (file count, size, track/album/artist counts).
7. File information (symbolic path, size) via a hash of the symbolic path for efficient lookup during resume operations.

### Search Capabilities

The index supports several search operations. Note that album and track paths returned or used in filtering are *symbolic*.

1. **Artist Search**
   ```python
   # Case-insensitive search by default
   artists = index.search_by_artist("artist")

   # Case-sensitive search
   artists = index.search_by_artist("Artist1", case_sensitive=True)

   # Fuzzy search (finds similar names when no exact match)
   artists = index.search_by_artist("Zemfra", fuzzy_search=True)
   ```

2. **Album Search**
   ```python
   # Search all albums (returns symbolic album paths)
   albums = index.search_by_album("Album")

   # Search albums by specific artist
   albums = index.search_by_album("Album", artist="Artist1")
   ```

3. **Track Search**
   ```python
   # Search all tracks by base name (returns TrackInfo objects)
   tracks = index.search_by_track("Track")

   # Search with filters (artist is exact, album is symbolic path)
   tracks = index.search_by_track("Track",
                                artist="Artist1",
                                album="LocationName/Artist1/Album1")
   ```

### Index Building

The index is built by scanning all configured dataset locations and grouping files by their components:

1. **Location Scanning**
   - Iterates through each location defined in `.blackbird/locations.json` (or the default 'Main' location).
   - Uses `os.walk` for efficient directory traversal within each location.
   - Shows real-time progress with tqdm.
   - Counts files and calculates total size across all locations.

2. **Component Matching & Base Name Extraction**
   - Matches files against component patterns defined in the schema.
   - Extracts the base track name by removing component suffixes.

3. **Symbolic Path Generation**
   - Creates symbolic paths by prepending the location name to the relative path within that location (e.g., `Main/Artist/Album/track_comp.ext`).

4. **Track Organization**
   - Groups related component files (represented by symbolic paths) under a unique symbolic track path (`Location/Artist/Album/[CD]/BaseName`).
   - Stores `TrackInfo` objects keyed by their symbolic track path.
   - Maintains mappings from artists to symbolic album paths and from symbolic album paths to symbolic track paths.
   - Stores file sizes keyed by their symbolic file path.

Example index building:
```python
# Build index with progress tracking (scans all locations)
index = DatasetIndex.build(dataset_root_path, schema)

# Save index
index.save(index_path)
```

### Synchronization with Index

The sync process uses the index for efficient file transfer:

1. **Component Selection**
   ```python
   dataset = Dataset(local_path)
   sync = DatasetSync(dataset)
   stats = sync.sync(
       client,
       components=["vocals", "mir"],
       artists=["Artist1"],
       albums=None,                  # optional album filter
       missing_component=None,       # only sync tracks missing this component
       resume=True,
       parallel=4,                   # concurrent download threads
       use_http2=False,              # enable HTTP/2
       connection_pool_size=10,
       target_location_name="Main",  # which location to download into
   )
   ```

2. **File Discovery**
   - Uses index instead of scanning remote server
   - Knows exact files and sizes upfront
   - Can calculate total size before starting

3. **Progress Tracking**
   ```python
   @dataclass
   class SyncStats:
       total_files: int = 0
       synced_files: int = 0
       failed_files: int = 0
       skipped_files: int = 0
       total_size: int = 0
       synced_size: int = 0
       downloaded_files: int = 0
       downloaded_size: int = 0
       profiling: Optional[ProfilingStats] = None
   ```

4. **Resume Support**
   - Verifies existing files by size
   - Skips correctly synced files
   - Tracks sync progress

5. **Error Handling**
   - Validates file sizes after download
   - Removes failed downloads
   - Provides detailed error reporting

Example sync output:
```
Collecting files to sync...
Found 1000 files to sync (50.5 GB)
Syncing files: 100% |████████| 1000/1000 [02:30<00:00, 6.67 files/s]

Sync completed!
Total files: 1000
Successfully synced: 950
Failed: 10
Skipped: 40
Total size: 50.5 GB
Synced size: 48.2 GB
```

### Remote Dataset Initialization

When connecting to a remote dataset, Blackbird follows a strict initialization sequence:

1. **Schema Download**
   ```
   Downloading schema from remote...
   Schema downloaded successfully.
   Available components:
   - instrumental (*.instrumental.mp3)
   - vocals_noreverb (*.vocals_noreverb.mp3)
   - mir (*.mir.json)
   ...
   ```

2. **Index Download**
   ```
   Downloading dataset index...
   Index downloaded successfully.
   Total tracks: 1000
   Total artists: 50
   ```

3. **Component Validation**
   - Before starting any sync/clone operation, validate requested components
   - If invalid components are requested, show available ones
   - Suggest similar component names using fuzzy matching
   ```
   Error: Unknown component 'vocal'
   Available components:
   - vocals_noreverb
   - vocals_stretched
   Did you mean 'vocals_noreverb'?
   ```

4. **Artist Validation**
   - Validate requested artists against the index
   - Show suggestions for similar artist names
   ```
   Error: Unknown artist 'Zemfira'
   Did you mean 'Земфира'?
   ```

This validation sequence ensures:
1. Users are aware of available components before sync starts
2. Typos in component or artist names are caught early
3. Helpful suggestions guide users to correct names
4. No unnecessary network traffic for invalid requests

