# Blackbird Dataset Manager Specification

## Overview

Blackbird Dataset Manager is a Python package designed to manage and synchronize music datasets with multiple components. It's specifically built to handle datasets where each track can have multiple associated files (instrumentals, vocals, MIR data, etc.) while maintaining a clear and consistent structure.

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
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section9.json'
'11.Юта - Жили-были (DJ ЦветкОFF Remix)_vocals_stretched_120bpm_section9.mp3'
```

## Core Concepts

### 1. Dataset Structure

The dataset follows a fixed hierarchical structure that is consistent across all installations:
```
dataset_root/
├── .blackbird/
│   ├── schema.json      # Component definitions only
│   └── sync_state.json  # Sync progress tracking
├── Artist1/
│   ├── Album1/
│   │   ├── track1_instrumental.mp3
│   │   ├── track1_vocals_noreverb.mp3
│   │   └── ...
│   └── Album2/
│       ├── CD1/        # Optional CD-level for multi-CD albums
│       │   ├── track1_instrumental.mp3
│       │   └── ...
│       └── CD2/
│           ├── track1_instrumental.mp3
│           └── ...
└── ...
```

The directory structure is fixed and non-configurable:
1. Artist level (required)
2. Album level (required)
3. CD level (optional, must match pattern "CD\\d+")
4. Track files with component-specific suffixes

### 2. Schema Management

The schema system defines ONLY the types of files (components) that can exist for each track. It does NOT define the directory structure, which is fixed. The schema specifies:
1. What component types exist (e.g., instrumentals, vocals, MIR data)
2. The naming pattern for each component type (including file extensions)
3. Whether multiple files of a component are allowed per track
4. Human-readable descriptions of each component's purpose

When syncing from a remote source, if the local schema does not have a component that exists remotely and was requested, the selective sync operation automatically updates the local schema. This ensures that different machines can maintain different subsets of components based on their needs and that tracking is handled by the schema file.

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
    
    # Access statistics
    stats = result.stats[name]
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

4. **Component Naming**
   - Component names are derived from file patterns
   - Special handling for audio files (adds `_audio` suffix)
   - Special handling for JSON files with lyrics (adds `_lyrics` suffix)
   - Section components get `_section` suffix

5. **Multiple Files Per Component**
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
result = schema.validate()

if result.is_valid:
    print("Schema validation successful")
    print(f"Total files: {result.stats['total_files']}")
    print(f"Matched files: {result.stats['matched_files']}")
else:
    print("Validation errors:")
    for error in result.errors:
        print(f"- {error}")
```

### 3. Synchronization

Blackbird uses WebDAV for dataset synchronization, with several key features:

#### 3.1 Pull-Only Design
- Only supports pulling from remote to local
- Rationale: Safer than bi-directional sync, prevents accidental data modification
- Each machine maintains its own subset of components

#### 3.2 Schema and Index Handling During Sync

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
# The schema and index are handled automatically
dataset.sync_from_remote(
    client,
    components=["vocals_audio", "mir"],  # Only sync these components
    artists=["Artist1"]
)
```

This process ensures that:
- The local schema always reflects the components actually present in the local dataset
- New components are added automatically when their files are synced
- The index stays in sync with the remote dataset
- No manual schema or index management is required

#### 3.4 Selective Component Sync
```python
dataset.sync_from_remote(
    client,
    components=["vocals", "mir"]  # Only sync specific components
)
```
- Can sync specific components instead of entire dataset
- Uses remote schema patterns to find relevant files
- Maintains component consistency between datasets

#### 3.5 Progress Tracking
- Real-time progress updates using tqdm
- Shows file counts and byte totals
- Displays transfer speed and estimated time remaining
- Reports errors with detailed messages

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
# Install package
pip install blackbird-dataset

# Basic usage
blackbird --help
```

### 1. Dataset Cloning
```bash
# Clone entire dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local

# Clone specific components
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --components vocals,mir

# Clone subset of artists (supports glob patterns)
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --artists "Artist1,Art*" \
    --components vocals

# Clone proportion of dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --proportion 0.1 \
    --offset 0
```

### 2. Dataset Analysis
```bash
# Show dataset statistics
blackbird stats /path/to/dataset

# Find incomplete tracks
blackbird find-tracks /path/to/dataset --missing vocals

# Rebuild dataset index
blackbird reindex /path/to/dataset
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
    """Clone dataset from remote source.
    
    URL: Remote dataset WebDAV URL (e.g. webdav://192.168.1.100:8080)
    DESTINATION: Local path for the cloned dataset
    
    The clone process follows these steps:
    1. Download and validate remote schema
    2. Download dataset index
    3. Validate requested components and artists
    4. Start file transfer
    """
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

## Dataset Indexing

Blackbird maintains a lightweight, fast index of the dataset for efficient operations. The index is stored in `.blackbird/index.pickle` using Python's pickle format with protocol 5 for optimal performance.

### Index Structure

```python
@dataclass
class TrackInfo:
    """Track information in the index."""
    track_path: str      # Relative path identifying the track (artist/album/[cd]/track)
    artist: str         # Artist name
    album_path: str     # Full path to album (artist/album)
    cd_number: Optional[str]  # CD number if present
    base_name: str      # Track name without component suffixes
    files: Dict[str, str]  # component_name -> file_path mapping
    file_sizes: Dict[str, int]  # file_path -> size in bytes

@dataclass
class DatasetIndex:
    """Main index structure."""
    last_updated: datetime
    tracks: Dict[str, TrackInfo]  # track_path -> TrackInfo
    track_by_album: Dict[str, Set[str]]  # album_path -> set of track_paths
    album_by_artist: Dict[str, Set[str]]  # artist_name -> set of album_paths
    total_size: int  # Total size of all indexed files
    version: str = "1.0"
```

The index provides efficient access to:
1. Track information by path
2. All tracks in an album
3. All albums by an artist
4. File sizes for verification during sync
5. Component files for each track

### Search Capabilities

The index supports several search operations:

1. **Artist Search**
   ```python
   # Case-insensitive search by default
   artists = index.search_by_artist("artist")
   
   # Case-sensitive search
   artists = index.search_by_artist("Artist1", case_sensitive=True)
   ```

2. **Album Search**
   ```python
   # Search all albums
   albums = index.search_by_album("Album")
   
   # Search albums by specific artist
   albums = index.search_by_album("Album", artist="Artist1")
   ```

3. **Track Search**
   ```python
   # Search all tracks
   tracks = index.search_by_track("Track")
   
   # Search with filters
   tracks = index.search_by_track("Track", 
                                artist="Artist1", 
                                album="Artist1/Album1")
   ```

### Index Building

The index is built by scanning the dataset and grouping files by their components:

1. **Directory Scanning**
   - Uses `os.walk` for efficient directory traversal
   - Shows real-time progress with tqdm
   - Counts files and calculates total size

2. **Component Grouping**
   - Groups files by their component patterns
   - Creates lookup tables for efficient access
   - Handles CD-based album structures

3. **Track Organization**
   - Groups related files by base name
   - Creates track paths for unique identification
   - Maintains file size information

Example index building:
```python
# Build index with progress tracking
index = build_index(dataset_path, schema)

# Save index
index.save(index_path)
```

### Synchronization with Index

The sync process uses the index for efficient file transfer:

1. **Component Selection**
   ```python
   sync = DatasetSync(local_path)
   stats = sync.sync(
       client,
       components=["vocals", "mir"],
       artists=["Artist1"],
       resume=True
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
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local

# Clone specific components
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --components vocals,mir

# Clone subset of artists (supports glob patterns)
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --artists "Artist1,Art*" \
    --components vocals

# Clone proportion of dataset
blackbird clone webdav://192.168.1.100:8080/dataset /path/to/local \
    --proportion 0.1 \
    --offset 0
```

### 2. Dataset Analysis
```bash
# Show dataset statistics
blackbird stats /path/to/dataset

# Find incomplete tracks
blackbird find-tracks /path/to/dataset --missing vocals

# Rebuild dataset index
blackbird reindex /path/to/dataset
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
    """Clone dataset from remote source.
    
    URL: Remote dataset WebDAV URL (e.g. webdav://192.168.1.100:8080)
    DESTINATION: Local path for the cloned dataset
    
    The clone process follows these steps:
    1. Download and validate remote schema
    2. Download dataset index
    3. Validate requested components and artists
    4. Start file transfer
    """
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

## Dataset Indexing

Blackbird maintains a lightweight, fast index of the dataset for efficient operations. The index is stored in `.blackbird/index.pickle` using Python's pickle format with protocol 5 for optimal performance.

### Index Structure

```python
@dataclass
class TrackInfo:
    """Track information in the index."""
    track_path: str      # Relative path identifying the track (artist/album/[cd]/track)
    artist: str         # Artist name
    album_path: str     # Full path to album (artist/album)
    cd_number: Optional[str]  # CD number if present
    base_name: str      # Track name without component suffixes
    files: Dict[str, str]  # component_name -> file_path mapping
    file_sizes: Dict[str, int]  # file_path -> size in bytes

@dataclass
class DatasetIndex:
    """Main index structure."""
    last_updated: datetime
    tracks: Dict[str, TrackInfo]  # track_path -> TrackInfo
    track_by_album: Dict[str, Set[str]]  # album_path -> set of track_paths
    album_by_artist: Dict[str, Set[str]]  # artist_name -> set of album_paths
    total_size: int  # Total size of all indexed files
    version: str = "1.0"
```

The index provides efficient access to:
1. Track information by path
2. All tracks in an album
3. All albums by an artist
4. File sizes for verification during sync
5. Component files for each track

### Search Capabilities

The index supports several search operations:

1. **Artist Search**
   ```python
   # Case-insensitive search by default
   artists = index.search_by_artist("artist")
   
   # Case-sensitive search
   artists = index.search_by_artist("Artist1", case_sensitive=True)
   ```

2. **Album Search**
   ```python
   # Search all albums
   albums = index.search_by_album("Album")
   
   # Search albums by specific artist
   albums = index.search_by_album("Album", artist="Artist1")
   ```

3. **Track Search**
   ```python
   # Search all tracks
   tracks = index.search_by_track("Track")
   
   # Search with filters
   tracks = index.search_by_track("Track", 
                                artist="Artist1", 
                                album="Artist1/Album1")
   ```

### Index Building

The index is built by scanning the dataset and grouping files by their components:

1. **Directory Scanning**
   - Uses `os.walk` for efficient directory traversal
   - Shows real-time progress with tqdm
   - Counts files and calculates total size

2. **Component Grouping**
   - Groups files by their component patterns
   - Creates lookup tables for efficient access
   - Handles CD-based album structures

3. **Track Organization**
   - Groups related files by base name
   - Creates track paths for unique identification
   - Maintains file size information

Example index building:
```python
# Build index with progress tracking
index = build_index(dataset_path, schema)

# Save index
index.save(index_path)
```

### Synchronization with Index

The sync process uses the index for efficient file transfer:

1. **Component Selection**
   ```python
   sync = DatasetSync(local_path)
   stats = sync.sync(
       client,
       components=["vocals", "mir"],
       artists=["Artist1"],
       resume=True
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