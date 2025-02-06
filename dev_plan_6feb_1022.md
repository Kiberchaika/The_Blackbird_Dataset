# Blackbird Dataset Manager Development Plan

## Project Description

Blackbird Dataset Manager is a Python package for managing, synchronizing, and analyzing music datasets with multiple optional components. The package is designed to handle datasets containing instrumental tracks, vocals, MIR analysis data, and cut sections, with flexibility to add new data types.

Key features:
- Flexible dataset structure supporting optional components
- WebDAV-based synchronization with selective component sync
- Schema management for dataset structure
- Statistics and analysis tools
- Command-line interface for common operations

## Full Specification

### 1. Dataset Structure

Base directory structure:
```
dataset_root/
├── .blackbird/
│   └── schema.json
├── Artist1/
│   ├── Album1/
│   │   ├── track1_instrumental.mp3
│   │   ├── track1_vocals_noreverb.mp3
│   │   ├── track1.mir.json
│   │   └── track1_vocals_stretched_120bpm_section1.mp3
│   └── Album2/
│       ├── CD1/
│       │   ├── track1_instrumental.mp3
│       │   └── ...
│       └── CD2/
│           ├── track1_instrumental.mp3
│           └── ...
└── ...
```

Schema structure (`schema.json`):
```json
{
  "version": "1.0",
  "components": {
    "instrumental": {
      "pattern": "*_instrumental.mp3",
      "required": true
    },
    "vocals": {
      "pattern": "*_vocals_noreverb.mp3",
      "required": false
    },
    "mir": {
      "pattern": "*.mir.json",
      "required": false
    },
    "sections": {
      "pattern": "*_vocals_stretched_120bpm_section*.mp3",
      "required": false,
      "multiple": true
    }
  },
  "structure": {
    "artist_album_format": {
      "levels": ["artist", "album", "?cd", "track"],
      "cd_pattern": "CD\\d+",
      "is_cd_optional": true
    }
  },
  "sync": {
    "default_components": ["instrumental", "vocals"],
    "exclude_patterns": ["*.tmp", "*.bak"]
  }
}
```

### 2. Core Package Structure

```
blackbird/
├── __init__.py
├── schema.py          # Schema management
├── dataset.py         # Dataset operations
├── sync.py           # WebDAV sync
├── stats.py          # Analysis tools
├── cli.py            # CLI implementation
└── utils.py          # Utilities
```

### 3. Python API

```python
from blackbird import Dataset, DatasetComponentSchema

# Initialize dataset
dataset = Dataset("/path/to/dataset")

# Schema operations
schema = dataset.schema
result = schema.add_component(
    "lyrics",
    pattern="*.lyrics.txt",
    required=False
)

# Find tracks
missing_mir = dataset.find_tracks(missing=['mir'])
complete = dataset.find_tracks(has=['vocals', 'mir'])

# Sync operations
dataset.sync(
    "webdav://server",
    components=['vocals', 'mir']
)

# Statistics
stats = dataset.analyze()
stats.print_summary()
```

### 4. Command Line Interface

```bash
# Initialize new dataset
blackbird init /path/to/dataset

# Show dataset status
blackbird status

# Add new component type
blackbird schema add lyrics "*.lyrics.txt" --required=false

# Sync specific components
blackbird sync --components=vocals,mir webdav://server

# Find tracks missing components
blackbird find --missing=mir

# Show dataset statistics
blackbird stats
```

## Development Plan

### Phase 1: Core Framework (Week 1)

1. Basic Package Setup
   - [ ] Create package structure
   - [ ] Setup.py configuration
   - [ ] Basic documentation
   - [ ] Development environment setup

2. Schema Implementation
   - [ ] DatasetComponentSchema class
   - [ ] Schema validation
   - [ ] Component management
   - [ ] Tests for schema operations

Test scenario:
```python
# Create test dataset structure
dataset_path = setup_test_dataset()
schema = DatasetComponentSchema.create(dataset_path)

# Test component operations
result = schema.add_component("test", "*.test")
assert result.is_valid
assert "test" in schema.components
```

### Phase 2: Dataset Operations (Week 2)

1. Dataset Management
   - [ ] Dataset class implementation
   - [ ] Track finding/filtering
   - [ ] File operations
   - [ ] Tests for dataset operations

2. Statistics Implementation
   - [ ] Port analysis script to use schema
   - [ ] Add component-based statistics
   - [ ] Tests for statistics

Test scenario:
```python
dataset = Dataset(dataset_path)
tracks = dataset.find_tracks(missing=['mir'])
stats = dataset.analyze()
assert len(tracks) == stats.missing_mir_count
```

### Phase 3: WebDAV Sync (Week 3)

1. Sync Implementation
   - [ ] WebDAV client setup
   - [ ] Selective sync logic
   - [ ] Progress tracking
   - [ ] Tests with mock WebDAV server

2. Sync State Management
   - [ ] Sync state tracking
   - [ ] Resume capability
   - [ ] Tests for state management

Test scenario:
```bash
# Setup test WebDAV server
docker run -d webdav-server

# Test sync
blackbird sync --components=vocals webdav://localhost
```

### Phase 4: CLI and Documentation (Week 4)

1. CLI Implementation
   - [ ] Command structure
   - [ ] Argument parsing
   - [ ] Progress display
   - [ ] Tests for CLI

2. Documentation
   - [ ] API documentation
   - [ ] CLI documentation
   - [ ] Usage examples
   - [ ] Tutorial for adding new components

Test scenario:
```bash
# Test CLI operations
blackbird init test_dataset
blackbird schema add lyrics "*.txt"
blackbird find --missing=lyrics
```

## Testing Strategy

1. Unit Tests
   - Schema operations
   - Dataset operations
   - File operations
   - Statistics calculations

2. Integration Tests
   - CLI operations
   - WebDAV sync
   - Full workflow scenarios

3. Test Environments
   - Local development
   - CI/CD pipeline
   - Test WebDAV server

4. Test Datasets
   - Minimal test dataset
   - Full example dataset
   - Edge case datasets

## Deployment Plan

1. Package Distribution
   - [ ] PyPI package setup
   - [ ] GitHub repository
   - [ ] CI/CD configuration
   - [ ] Version management

2. Documentation Deployment
   - [ ] ReadTheDocs setup
   - [ ] API documentation
   - [ ] Usage examples
   - [ ] Tutorials

## Success Criteria

1. Core Functionality
   - Schema management works correctly
   - Dataset operations are reliable
   - WebDAV sync is stable
   - CLI is user-friendly

2. Performance
   - Fast operation on large datasets
   - Efficient sync with large files
   - Low memory usage

3. User Experience
   - Clear error messages
   - Helpful CLI output
   - Good documentation
   - Easy installation

4. Code Quality
   - High test coverage
   - Clean code structure
   - Good documentation
   - Type hints and validation 