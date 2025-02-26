# Blackbird Dataset Manager

![Blackbird Dataset Manager](https://github.com/Kiberchaika/The_Blackbird_Dataset/blob/0b2b11f6de602f97d1b4f65fdb1164d7cd5e37b0/blackbird.jpg)


This is a tool to manage, synchronize and otherwise work with the Blackbird music dataset.

## Features

- Flexible dataset structure supporting optional components - various file types per track
- WebDAV-based synchronization with selective component sync
- Schema management for dataset structure
- Statistics and analysis tools
- Command-line interface for common operations
- Performance optimizations:
  - Parallel downloads with multi-threading
  - HTTP/2 support for faster connections
  - Connection pooling for reduced overhead
  - Detailed performance profiling

## TODO

- Continue after download stopped abruptly, and cover that with tests
It should already skip files that are already downloaded, but that wasn't covered by tests and checked

## Installation

There are two ways to install the Blackbird Dataset Manager:

### 1. Using pip (recommended)

```bash
# Install from the repository root
pip install -e .
```

### 2. Using requirements.txt

```bash
# Install dependencies first
pip install -r requirements.txt

# Then install the package
pip install -e .
```

## Usage

After installation, you can use the `blackbird` command-line tool:

```bash
# Show help
blackbird --help

# Clone a dataset (downloads from remote to a new local dataset)
blackbird clone webdav://server/dataset /path/to/local --components vocals,instrumental --parallel 4

# Sync a dataset (updates existing local dataset)
blackbird sync webdav://server/dataset /path/to/local --components vocals,instrumental --parallel 4

# Clone only for tracks missing a specific component
blackbird clone webdav://server/dataset /path/to/local --components vocals,instrumental --missing mir
# This will only download vocals and instrumental files for tracks that don't have mir files

# Show dataset statistics
blackbird stats /path/to/dataset

# Find tracks
blackbird find-tracks /path/to/dataset --missing vocals

# Show schema
blackbird schema show /path/to/dataset

# Add new component
blackbird schema add /path/to/dataset component_name "*_pattern.mp3"

# Rebuild index
blackbird reindex /path/to/dataset
```

## Performance Optimizations

The Blackbird Dataset Manager includes several performance optimizations for faster downloads:

### Parallel Downloads

Use the `--parallel` option to enable multi-threaded downloads:

```bash
# Download with 4 parallel threads
blackbird sync webdav://server/dataset /path/to/local --parallel 4
```

### HTTP/2 Support

Enable HTTP/2 for faster connections with the `--http2` flag:

```bash
# Use HTTP/2 protocol (requires httpx package)
blackbird sync webdav://server/dataset /path/to/local --http2
```

### Connection Pooling

Adjust connection pool size with the `--connection-pool` option:

```bash
# Set connection pool size to 20
blackbird sync webdav://server/dataset /path/to/local --connection-pool 20
```

### Performance Profiling

Enable performance profiling to identify bottlenecks:

```bash
# Enable profiling
blackbird sync webdav://server/dataset /path/to/local --profile
```

### Combining Optimizations

For best performance, combine all optimizations:

```bash
# Use all optimizations
blackbird sync webdav://server/dataset /path/to/local --parallel 4 --http2 --connection-pool 20 --profile
```

## Development

For development, install additional dependencies:

```bash
pip install -e ".[dev]"
```

This will install development tools like pytest, black, and mypy.

## Quick Start

```python
from blackbird import Dataset

# Initialize dataset
dataset = Dataset("/path/to/dataset")

# Find tracks missing MIR data
missing_mir = dataset.find_tracks(missing=['mir'])

# Sync specific components
dataset.sync("webdav://server", components=['vocals', 'mir'])

# Sync with performance optimizations
dataset.sync("webdav://server", 
             components=['vocals', 'mir'],
             parallel=4,
             use_http2=True,
             connection_pool_size=20)
```

## Command Line Usage

```bash
# Initialize new dataset
blackbird init /path/to/dataset

# Show dataset status
blackbird status

# Add new component type
blackbird schema add lyrics "*.lyrics.txt"

# Sync specific components
blackbird sync --components=vocals,mir webdav://server
```

## Dataset Structure

The dataset follows a hierarchical structure:
```
dataset_root/
├── Artist1/
│   ├── Album1/
│   │   ├── track1_instrumental.mp3
│   │   └── ...
│   └── Album2/
│       ├── CD1/
│       │   ├── track1_instrumental.mp3
│       │   └── ...
│       └── CD2/
│           ├── track1_instrumental.mp3
│           └── ...
└── ...
```

## Performance Testing

The repository includes a test script to compare different optimization strategies:

```bash
# Run performance tests
python test_optimized_sync.py webdav://server/dataset
```

This will generate a performance comparison chart showing the impact of different optimizations.

## License

MIT License