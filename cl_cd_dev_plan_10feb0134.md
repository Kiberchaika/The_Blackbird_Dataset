# Blackbird CLI Development Plan

## Overview

The Blackbird CLI will provide essential dataset management operations through a simple command-line interface. The CLI will be implemented using Click for argument parsing and command organization.

## Commands Structure

```bash
blackbird
├── discover     # Discover and save schema, build cache
├── cache        # Cache management commands
│   ├── update   # Update cache for current dataset
│   └── rebuild  # Rebuild cache from scratch
├── sync        # Sync dataset from remote
└── setup       # Setup commands
    └── server  # Setup WebDAV server
```

## Command Details

### 1. Schema Discovery and Cache Building

```bash
# Basic usage
blackbird discover /path/to/dataset

# With options
blackbird discover /path/to/dataset \
    --num-artists 25 \
    --build-cache \
    --force  # Override existing schema
```

Implementation Plan:
1. Port existing `discover_and_save_schema.py` functionality
2. Add cache building step after schema discovery
3. Add validation of dataset path
4. Add confirmation for existing schema override
5. Progress reporting for both schema discovery and cache building

Example output:
```
Analyzing dataset at /path/to/dataset...
Selected 25 random artists for analysis
Discovering schema... done
Building cache... done

Schema saved to /path/to/dataset/.blackbird/schema.json
Cache saved to /path/to/dataset/.blackbird/index.pickle

Summary:
- Components found: 5
- Total tracks: 1000
- Total size: 50.5 GB
```

### 2. Cache Update

```bash
# Update cache in current dataset
blackbird cache update

# Update cache in specific dataset
blackbird cache update /path/to/dataset

# Rebuild cache from scratch
blackbird cache rebuild
```

Implementation Plan:
1. Detect current dataset if no path provided
2. Validate existing schema and cache
3. Implement incremental update (only scan changed files)
4. Implement full rebuild option
5. Progress reporting with ETA

Example output:
```
Updating cache for dataset at /path/to/dataset
Scanning for changes... found 50 new files
Updating index... done

Cache updated successfully!
- Added: 45 files
- Modified: 5 files
- Removed: 0 files
```

### 3. Remote Sync

```bash
# Basic sync
blackbird sync webdav://server/dataset

# Sync specific components
blackbird sync webdav://server/dataset \
    --components vocals,mir \
    --artists "Artist1,Artist2" \
    --resume

# Create new dataset
blackbird sync webdav://server/dataset /path/to/new/dataset
```

Implementation Plan:
1. Support both existing and new datasets
2. Confirmation prompt for new dataset creation
3. Schema and cache handling for new datasets
4. Resume support with state tracking
5. Component and artist filtering
6. Progress reporting with speed and ETA

Example output:
```
Target directory /path/to/new/dataset does not exist.
Create new dataset? [y/N]: y

Creating new dataset...
Fetching remote schema... done
Building local cache... done

Syncing files:
[##########] 100/500 files | 2.5 GB/12.5 GB | 50 MB/s | ETA: 3:30
```

### 4. Server Setup Wizard

```bash
# Start server setup wizard
blackbird setup server

# With options
blackbird setup server \
    --path /path/to/dataset \
    --port 8080 \
    --ssl
```

Implementation Plan:
1. Interactive wizard for server setup
2. System requirements check
3. Nginx installation and configuration
4. SSL certificate setup (optional)
5. User authentication setup
6. Testing and validation

Example wizard flow:
```
Blackbird WebDAV Server Setup
============================

Checking system requirements...
- nginx: not installed (will be installed)
- nginx-dav-ext-module: not installed (will be installed)

Dataset location: [/path/to/dataset] 
Server port: [8080] 
Enable SSL? [y/N] y
Generate self-signed certificate? [Y/n] 

Setting up server...
1. Installing packages... done
2. Configuring nginx... done
3. Setting up SSL... done
4. Creating user credentials... done
5. Setting permissions... done
6. Starting services... done

Testing configuration...
- WebDAV connection: OK
- File access: OK
- SSL certificate: OK

Server setup complete!
WebDAV URL: https://localhost:8080
```

## Implementation Guidelines

### 1. Code Organization

```python
blackbird/
├── cli/
│   ├── __init__.py
│   ├── discover.py    # Schema discovery commands
│   ├── cache.py       # Cache management commands
│   ├── sync.py        # Sync commands
│   ├── setup.py       # Setup commands
│   └── utils.py       # CLI utilities
```

### 2. Error Handling

1. Clear error messages with context
2. Graceful failure handling
3. Cleanup on failure
4. User-friendly suggestions

### 3. Progress Reporting

1. Use tqdm for progress bars
2. Show operation details (files, sizes, speeds)
3. Provide accurate ETA
4. Support both interactive and non-interactive modes

### 4. Testing

1. Unit tests for each command
2. Integration tests for full workflows
3. Mock WebDAV server for sync testing
4. Test server setup in container

## Development Phases

### Phase 1: Core Commands
1. Implement discover command
2. Implement cache update
3. Basic error handling
4. Progress reporting

### Phase 2: Sync Implementation
1. Implement sync command
2. Resume functionality
3. State tracking
4. Component filtering

### Phase 3: Server Setup
1. Implement server wizard
2. Nginx configuration
3. SSL handling
4. Testing tools

### Phase 4: Polish
1. Improve error messages
2. Add command aliases
3. Add shell completion
4. Documentation 