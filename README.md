# Blackbird Dataset Manager

A Python package for managing, synchronizing, and analyzing music datasets with multiple components.

## Features

- Flexible dataset structure supporting optional components
- WebDAV-based synchronization with selective component sync
- Schema management for dataset structure
- Statistics and analysis tools
- Command-line interface for common operations

## Installation

```bash
pip install blackbird-dataset
```

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

## Development

```bash
# Clone the repository
git clone https://github.com/yourusername/blackbird-dataset.git
cd blackbird-dataset

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

## License

MIT License