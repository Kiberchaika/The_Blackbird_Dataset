# Blackbird Dataset Manager

A tool for managing music datasets with multiple components, supporting efficient synchronization and component management.

## Features

- Flexible dataset structure supporting optional components
- WebDAV-based synchronization with selective component sync
- Schema management for dataset structure
- Statistics and analysis tools
- Command-line interface for common operations

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

# Clone a dataset
blackbird clone webdav://server/dataset /path/to/local --components vocals,instrumental

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
```

## Command Line Usage

```bash
# Initialize new dataset
blackbird init /path/to/dataset

# Show dataset status
blackbird status

# Add new component type
blackbird schema add lyrics "*.lyrics.txt" --required=false

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

## License

MIT License