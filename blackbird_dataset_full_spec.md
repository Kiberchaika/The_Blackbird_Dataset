# Blackbird Dataset Manager Specification

## Overview

Blackbird Dataset Manager is a Python package designed to manage and synchronize music datasets with multiple components. It's specifically built to handle datasets where each track can have multiple associated files (instrumentals, vocals, MIR data, etc.) while maintaining a clear and consistent structure.

## Core Concepts

### 1. Dataset Structure

The dataset follows a hierarchical structure:
```
dataset_root/
├── .blackbird/
│   ├── schema.json
│   └── sync_state.json
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

Key features:
- Artist/Album organization
- Optional CD subdirectories for multi-CD albums
- Component files follow consistent naming patterns
- Hidden `.blackbird` directory for metadata

### 2. Schema Management

The schema system is the core of Blackbird's functionality. It defines:
1. What components exist in the dataset
2. How to identify files belonging to each component
3. Which components are required vs optional
4. Rules for directory structure

Example schema (`schema.json`):
```json
{
  "version": "1.0",
  "components": {
    "instrumental": {
      "pattern": "*_instrumental.mp3",
      "required": true,
      "description": "Instrumental tracks"
    },
    "vocals": {
      "pattern": "*_vocals_noreverb.mp3",
      "required": false,
      "description": "Isolated vocals without reverb"
    },
    "mir": {
      "pattern": "*.mir.json",
      "required": false,
      "description": "Music Information Retrieval analysis"
    },
    "sections": {
      "pattern": "*_vocals_stretched_120bpm_section*.mp3",
      "required": false,
      "multiple": true,
      "description": "Cut sections of vocals stretched to 120 BPM"
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

#### Schema Design Rationale

1. **Component Definitions**
   - `pattern`: Uses glob patterns for flexible file matching
   - `required`: Distinguishes core components from optional ones
   - `multiple`: Allows components that can have multiple files per track (e.g., sections)
   - `description`: Documents component purpose

2. **Directory Structure Rules**
   - Explicit level definitions ensure consistent organization
   - CD pattern validation maintains uniformity across multi-CD albums
   - Optional CD level accommodates both single and multi-CD albums

3. **Sync Configuration**
   - Default components list for common sync operations
   - Exclude patterns prevent syncing temporary/backup files

### 3. Synchronization

Blackbird uses WebDAV for dataset synchronization, with several key features:

#### 3.1 Pull-Only Design
- Only supports pulling from remote to local
- Rationale: Safer than bi-directional sync, prevents accidental data modification
- Each machine maintains its own subset of components

#### 3.2 Schema-First Sync
1. First pulls and updates schema from remote
2. Uses remote schema to identify files to sync
3. Fails fast if requested components don't exist in remote
   - Rationale: Prevents partial/incomplete syncs
   - Better to fail early than have inconsistent state

#### 3.3 Selective Component Sync
```python
dataset.sync_from_remote(
    client,
    components=["vocals", "mir"]  # Only sync specific components
)
```
- Can sync specific components instead of entire dataset
- Uses remote schema patterns to find relevant files
- Maintains component consistency between datasets

#### 3.4 Resumable Operations
- Tracks sync state in `sync_state.json`
- Records successfully synced files
- Can resume interrupted syncs
- Tracks progress and completed bytes

#### 3.5 Progress Tracking
- Real-time progress updates
- File counts and byte totals
- Error tracking with detailed messages
- Resumable from last successful file

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

Each component serves a specific purpose:
1. **Instrumental** - The base instrumental track (required)
2. **Vocals** - Isolated vocals without reverb
3. **MIR** - Music Information Retrieval analysis data
4. **Sections** - Cut sections of vocals, time-stretched to 120 BPM
5. **Lyrics** - Timing information and lyrics text

### Component Definition Properties

#### 1. Pattern (`pattern`)
- Uses glob patterns for file matching
- Must uniquely identify files of this component type
- Examples:
  ```json
  {
    "instrumental": {"pattern": "*_instrumental.mp3"},
    "vocals": {"pattern": "*_vocals_noreverb.mp3"},
    "mir": {"pattern": "*.mir.json"},
    "sections": {"pattern": "*_vocals_stretched_120bpm_section*.mp3"}
  }
  ```
- Test coverage: `test_add_component` verifies pattern matching

#### 2. Required Flag (`required`)
- Indicates if component must exist for all tracks
- Used for dataset validation
- Examples:
  - `instrumental`: `required=true` (base tracks must exist)
  - `vocals`: `required=false` (some tracks may not have vocals)
- Test coverage: `test_validate_structure` checks required components

#### 3. Multiple Files Flag (`multiple`)
- Allows multiple files per track for this component
- Examples:
  - `sections`: `multiple=true` (multiple cut sections per track)
  - `instrumental`: `multiple=false` (one instrumental per track)
- Test coverage: Needs new test for multiple file validation

#### 4. Description (`description`)
- Documents the purpose and format of the component
- Helps users understand what each component represents
- Example: `"Isolated vocals without reverb"`
- Test coverage: Not tested (metadata only)

### Naming Pattern Role

Naming patterns serve multiple purposes:
1. **Component Identification** - Reliably identify file types
2. **Track Grouping** - Group related files by base name
3. **Validation** - Ensure consistent file organization
4. **Sync Selection** - Enable selective component syncing

Example pattern matching:
```python
# Base name: "01 - Track Name"
"01 - Track Name_instrumental.mp3"      # Matches instrumental pattern
"01 - Track Name_vocals_noreverb.mp3"   # Matches vocals pattern
"01 - Track Name.mir.json"              # Matches MIR pattern
```

## Synchronization

### Pull-Only Design

Blackbird implements a pull-only sync design where:
1. Each machine pulls from remote sources
2. No pushing to remote is allowed
3. Schema is pulled first, then files

#### Fail-Fast Behavior
"Fail-fast" means immediately stopping the operation when an error is detected, rather than trying to continue partially:

1. Schema Validation
   ```python
   # If requesting non-existent components
   sync.sync_from_remote(client, components=["nonexistent"])
   # Raises ValueError immediately with available components list
   ```
   Test coverage: `test_sync_with_missing_component`

2. File Validation
   ```python
   # If remote files don't match patterns
   # Fails before any downloads start
   ```
   Test coverage: Needs new test

### Selective Sync Features

#### 1. Component Selection
```python
sync.sync_from_remote(client, components=["vocals", "mir"])
```
Test coverage: `test_selective_component_pull`

#### 2. Artist Selection
```python
# Exact match
sync.sync_from_remote(client, artists=["Artist1"])
# Fuzzy match
sync.sync_from_remote(client, artists=["Art*"])
```
Test coverage: `test_find_tracks_by_artist` (for finding, needs sync test)

#### 3. Dataset Proportion
```python
# Sync 10% of dataset starting from offset 0
sync.sync_from_remote(client, proportion=0.1, offset=0)
```
Test coverage: Needs new test

#### 4. Combined Filters
```python
sync.sync_from_remote(
    client,
    components=["vocals"],
    artists=["Artist1"],
    proportion=0.1
)
```
Test coverage: Needs new test

### Progress Tracking

1. Schema Update
   ```
   Updating schema from remote...
   ```

2. File Discovery
   ```
   Finding remote files to sync...
   Found 1000 files matching patterns
   ```

3. Download Progress
   ```
   Downloading files: 50/1000 [===>  ] 5%
   ```

Test coverage: Progress callback testing needs to be added

## Implementation Guidelines

### 1. Error Handling
- Fail fast on critical errors (e.g., missing remote components)
- Detailed error messages with context
- Track partial failures during sync
- Allow resume after transient failures

### 2. Performance Considerations
- Efficient file pattern matching
- Progress tracking for long operations
- Resumable operations for large syncs
- Memory-efficient file handling

### 3. Data Integrity
- Schema validation before operations
- Sync state tracking
- Component consistency checks
- Directory structure validation

## Use Cases

1. **Initial Dataset Setup**
   ```python
   dataset = Dataset("/path/to/dataset")
   dataset.schema.add_component("vocals", "*_vocals_noreverb.mp3")
   ```

2. **Selective Sync**
   ```python
   dataset.sync_from_remote(
       client,
       components=["vocals", "mir"],
       resume=True
   )
   ```

3. **Dataset Analysis**
   ```python
   stats = dataset.analyze()
   print(f"Tracks with vocals: {stats['components']['vocals']}")
   ```

4. **Track Finding**
   ```python
   # Find tracks missing MIR data
   missing_mir = dataset.find_tracks(missing=["mir"])
   ```

## Command Line Interface

Blackbird provides a comprehensive CLI for dataset operations after pip installation:

```bash
# Install package
pip install blackbird-dataset

# Basic usage
blackbird --help
```

### 1. Dataset Cloning
```bash
# Clone entire dataset
blackbird clone webdav://192.168.1.100/dataset /path/to/local

# Clone specific components
blackbird clone webdav://192.168.1.100/dataset /path/to/local \
    --components vocals,mir

# Clone subset of artists (supports glob patterns)
blackbird clone webdav://192.168.1.100/dataset /path/to/local \
    --artists "Artist1,Art*" \
    --components vocals

# Clone proportion of dataset
blackbird clone webdav://192.168.1.100/dataset /path/to/local \
    --proportion 0.1 \
    --offset 0
```

### 2. Dataset Analysis
```bash
# Show dataset statistics
blackbird stats /path/to/dataset

# Find incomplete tracks
blackbird find-tracks /path/to/dataset --missing vocals
```

### 3. Schema Management
```bash
# Show current schema
blackbird schema show /path/to/dataset

# Add new component
blackbird schema add /path/to/dataset \
    --name lyrics \
    --pattern "*.lyrics.json"
```

## WebDAV Server Setup

Blackbird includes a wizard for setting up a WebDAV server on Ubuntu using nginx:

```bash
# Start WebDAV setup wizard
blackbird setup-server /path/to/dataset
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
   # Verify WebDAV access
   client = blackbird.configure_client("webdav://localhost")
   assert client.check_connection()
   ```

2. **File Access Test**
   ```python
   # Verify file listing
   files = client.list()
   assert len(files) > 0
   
   # Verify file download
   test_file = files[0]
   assert client.download_sync(test_file, "test.tmp")
   ```

3. **Performance Test**
   ```python
   # Test download speed
   speed = client.test_download_speed()
   print(f"Download speed: {speed} MB/s")
   ```

### CLI Implementation

The CLI is implemented using Click:

```python
@click.group()
def cli():
    """Blackbird Dataset Manager CLI"""
    pass

@cli.command()
@click.argument('url')
@click.argument('destination')
@click.option('--components', help='Comma-separated list of components')
@click.option('--artists', help='Comma-separated list of artists (glob patterns supported)')
@click.option('--proportion', type=float, help='Proportion of dataset to sync (0-1)')
@click.option('--offset', type=int, help='Offset for partial sync')
def clone(url, destination, components, artists, proportion, offset):
    """Clone dataset from WebDAV URL"""
    components = components.split(',') if components else None
    artists = artists.split(',') if artists else None
    
    dataset = Dataset(destination)
    client = dataset.configure_client(url)
    
    dataset.sync_from_remote(
        client,
        components=components,
        artists=artists,
        proportion=proportion,
        offset=offset
    )

@cli.command()
@click.argument('path')
def setup_server(path):
    """Setup WebDAV server for dataset"""
    wizard = ServerSetupWizard(path)
    wizard.run()
```

Test coverage needed:
1. CLI command testing
2. WebDAV server setup testing
3. Server configuration validation
4. Connection testing
5. Security testing 