# The Blackbird Dataset

![The Blackbird Dataset](https://github.com/Kiberchaika/The_Blackbird_Dataset/blob/0b2b11f6de602f97d1b4f65fdb1164d7cd5e37b0/blackbird.jpg)

A Python package for managing and synchronizing music datasets with multiple components. Built to handle datasets where each track can have multiple associated files (instrumentals, vocals, MIR data, etc.) while maintaining a clear and consistent structure.

Supports distributing the dataset across multiple storage locations and provides a streaming pipeline for processing remote datasets without downloading everything upfront.

## Features

- **Multi-location storage** — distribute dataset across multiple drives, configured via `.blackbird/locations.json`
- **Component-based schema** — flexible schema system defining file types per track (instrumentals, vocals, MIR data, lyrics, etc.)
- **WebDAV synchronization** — pull-based sync with selective component and artist/album filtering
- **Streaming pipeline** — download, process, and upload results back without storing the full dataset locally
- **Resume support** — interrupted clone/sync operations can be resumed from state files
- **Performance optimizations** — parallel downloads, HTTP/2, connection pooling, profiling
- **CLI and Python API** — full-featured command-line interface and programmatic access

## Installation

```bash
# Install from the repository root
pip install -e .

# Or with development dependencies
pip install -e ".[dev]"
```

## Dataset Structure

The dataset follows a fixed hierarchical structure within each configured storage location:

```
location_root/           # e.g., /mnt/hdd/dataset or /mnt/ssd/dataset_part
├── Artist1/
│   ├── Album1/
│   │   ├── track1_instrumental.mp3
│   │   ├── track1_vocals_noreverb.mp3
│   │   ├── track1.mir.json
│   │   └── ...
│   └── Album2/
│       ├── CD1/         # Optional CD-level for multi-CD albums
│       │   ├── track1_instrumental.mp3
│       │   └── ...
│       └── CD2/
│           ├── track1_instrumental.mp3
│           └── ...
└── ...
```

Multiple storage locations are defined in `.blackbird/locations.json`:

```json
{
  "Main": "/path/to/primary/storage",
  "SSD_Fast": "/path/to/secondary/storage"
}
```

If this file doesn't exist, a single location named "Main" pointing to the dataset root is assumed. The dataset index stores file paths symbolically, prepending the location name (e.g., `Main/Artist1/Album1/track1_instrumental.mp3`).

## Schema

The schema defines the types of files (components) that can exist for each track. It does **not** define the directory structure, which is fixed.

Example `.blackbird/schema.json`:

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
    "instrumental_audio": {
      "pattern": "*_instrumental.mp3",
      "multiple": false,
      "description": "Instrumental file with vocals removed"
    },
    "vocals_stretched_audio": {
      "pattern": "*_vocals_stretched.mp3",
      "multiple": true,
      "description": "Vocals file stretched to 120bpm"
    }
  }
}
```

Schemas can be auto-discovered from existing files:

```bash
blackbird schema discover /path/to/dataset --num-artists 25
```

When syncing from a remote source, missing components are automatically added to the local schema.

## Command Line Interface

```bash
blackbird --help
```

Running `blackbird` inside a dataset directory (without a subcommand) displays the current dataset status.

### Clone

```bash
# Clone entire dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local

# Clone specific components
blackbird clone webdav://server/dataset /path/to/local --components vocals,mir

# Clone only for tracks missing a specific component
blackbird clone webdav://server/dataset /path/to/local --components vocals,mir --missing caption

# Clone subset of artists (glob patterns supported)
blackbird clone webdav://server/dataset /path/to/local --artists "Artist1,Art*"

# Clone a proportion of the dataset
blackbird clone webdav://server/dataset /path/to/local --proportion 0.1 --offset 0

# With performance options
blackbird clone webdav://server/dataset /path/to/local \
    --parallel 4 --http2 --connection-pool 20 --target-location Main
```

### Sync

```bash
# Sync existing local dataset from remote
blackbird sync webdav://server/dataset /path/to/local --components vocals,mir

# Sync with artist and album filters
blackbird sync webdav://server/dataset /path/to/local \
    --artists "Artist1" --albums "Album1,Album2"

# Force reindex before syncing
blackbird sync webdav://server/dataset /path/to/local --force-reindex
```

### Resume Interrupted Operations

```bash
blackbird resume /path/to/.blackbird/operation_sync_1234567890.json
blackbird resume /path/to/state_file.json --parallel 4 --http2
```

### Statistics and Search

```bash
# Show dataset statistics (local or remote)
blackbird stats /path/to/dataset
blackbird stats webdav://server/dataset

# Stats for tracks missing a component
blackbird stats /path/to/dataset --missing vocals

# Find tracks by component presence
blackbird find-tracks /path/to/dataset --missing vocals
blackbird find-tracks /path/to/dataset --has instrumental --missing caption
blackbird find-tracks /path/to/dataset --artist "Artist1" --album "Album1"

# Rebuild dataset index
blackbird reindex /path/to/dataset
```

### Schema Management

```bash
# Show current schema (local or remote)
blackbird schema show /path/to/dataset
blackbird schema show webdav://server/dataset

# Discover and save schema automatically
blackbird schema discover /path/to/dataset --num-artists 25

# Add new component
blackbird schema add /path/to/dataset lyrics "*.lyrics.json"
blackbird schema add /path/to/dataset section_audio "*_section*.mp3" --multiple
```

### Location Management

```bash
# List configured storage locations
blackbird location list /path/to/dataset

# Add a new storage location
blackbird location add /path/to/dataset SSD_Fast /mnt/fast_storage/dataset_part

# Remove a location
blackbird location remove /path/to/dataset SSD_Fast

# Move folders between locations
blackbird location move-folders /path/to/dataset Main \
    "Artist1/Album1,Artist2/Album B/CD1" --source-location SSD_Fast

# Balance storage: move ~100GB from Main to SSD_Fast
blackbird location balance /path/to/dataset Main SSD_Fast --size 100 --dry-run
```

### WebDAV Server Setup

```bash
# Start WebDAV setup wizard (nginx-based, Ubuntu)
blackbird webdav setup /path/to/dataset --port 8080
```

## Python API

### Basic Usage

```python
from blackbird.dataset import Dataset
from blackbird.sync import DatasetSync, configure_client, clone_dataset

# Clone a dataset
stats = clone_dataset(
    source_url="webdav://server/dataset",
    destination=Path("/path/to/local"),
    components=["vocals", "mir"],
    artists=["Artist1"],
    parallel=4,
    use_http2=True,
)

# Sync an existing dataset
dataset = Dataset("/path/to/local")
client = configure_client("webdav://server/dataset")
sync = DatasetSync(dataset)
stats = sync.sync(client, components=["vocals", "mir"], parallel=4)
```

### Streaming Pipeline

Process a remote dataset without downloading everything upfront. Files are downloaded into a bounded queue, processed locally, and results are uploaded back.

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

The pipeline supports resume (via `.pipeline_state.json`), retry with exponential backoff, and graceful shutdown on Ctrl+C.

## Running Tests

```bash
python -m pytest                              # Run all tests
python -m pytest -v                           # Verbose output
python -m pytest blackbird/tests/test_locations.py  # Specific module
python -m pytest --cov=blackbird              # With coverage
```

## License

MIT License
