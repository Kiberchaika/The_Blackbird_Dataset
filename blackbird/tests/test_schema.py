import pytest
from pathlib import Path
import shutil
import json
from blackbird.schema import DatasetComponentSchema
import os
from blackbird.dataset import Dataset

@pytest.fixture
def test_dataset():
    """Create a test dataset structure."""
    # Use a fixed path instead of tmp_path
    dataset_root = Path("/tmp/blackbird_test_dataset")
    print(f"\nCreating test dataset at: {dataset_root}")
    
    # Clean up any existing test data
    if dataset_root.exists():
        print("Cleaning up existing test data...")
        shutil.rmtree(dataset_root)
    
    # Create basic structure
    print("\nCreating directory structure:")
    dataset_root.mkdir(parents=True)
    
    return dataset_root

def test_schema_creation(test_dataset):
    """Test creating a new schema."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    assert schema.schema_path.exists()
    with open(schema.schema_path) as f:
        data = json.load(f)
    
    assert "version" in data
    assert "components" in data
    assert isinstance(data["components"], dict)

def test_add_component(test_dataset):
    """Test adding a new component."""
    print("\n=== Starting test_add_component ===")
    print(f"Test dataset path: {test_dataset}")
    
    print("\nStep 1: Creating schema")
    schema = DatasetComponentSchema.create(test_dataset)
    print(f"Schema file created at: {schema.schema_path}")
    
    print("\nStep 2: Initial schema components")
    for name, config in schema.schema["components"].items():
        print(f"- {name}:")
        for key, value in config.items():
            print(f"    {key}: {value}")
    
    print("\nStep 3: Adding vocals_noreverb component")
    print("Parameters:")
    print("- name: vocals_noreverb")
    print("- pattern: *_vocals_noreverb.mp3")
    print("- required: False")
    
    result = schema.add_component(
        "vocals_noreverb",
        pattern="*_vocals_noreverb.mp3",
        required=False
    )
    
    print("\nStep 4: Validation result")
    print(f"Is valid: {result.is_valid}")
    if not result.is_valid:
        print("\nErrors found:")
        for error in result.errors:
            print(f"- {error}")
    
    print("\nValidation statistics:")
    for key, value in result.stats.items():
        if key == "component_coverage":
            print("\nComponent coverage:")
            for comp_name, coverage in value.items():
                print(f"\n  {comp_name}:")
                for stat_key, stat_value in coverage.items():
                    print(f"    {stat_key}: {stat_value}")
        else:
            print(f"{key}: {value}")
    
    print("\nStep 5: Final schema components")
    for name, config in schema.schema["components"].items():
        print(f"\n{name}:")
        for key, value in config.items():
            print(f"  {key}: {value}")
    
    print("\nStep 6: Running assertions")
    assert result.is_valid, "Validation failed"
    assert result.stats["matched_files"] == 0, "Expected 0 matched files"
    assert "vocals_noreverb" in schema.schema["components"], "Component not added to schema"
    assert schema.schema["components"]["vocals_noreverb"]["pattern"] == "*_vocals_noreverb.mp3", "Wrong pattern"
    
    print("\nStep 7: Testing invalid component name")
    print("Attempting to add component with invalid name: 'invalid name'")
    result = schema.add_component(
        "invalid name",
        pattern="*.txt"
    )
    
    print("\nInvalid component validation result:")
    print(f"Is valid: {result.is_valid}")
    if not result.is_valid:
        print("Errors:")
        for error in result.errors:
            print(f"- {error}")
    
    assert not result.is_valid, "Expected validation to fail for invalid name"
    assert "Invalid component name" in result.errors[0], "Expected 'Invalid component name' error"
    
    print("\n=== test_add_component completed ===")
    print(f"Test data remains at: {test_dataset}")
    print("You can inspect the test data and schema at this location.")

def test_remove_component(test_dataset):
    """Test removing a component."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add and then remove a component
    schema.add_component("vocals_noreverb", "*_vocals_noreverb.mp3")
    result = schema.remove_component("vocals_noreverb")
    
    assert result.is_valid
    assert "vocals_noreverb" not in schema.schema["components"]
    
    # Try removing required component
    result = schema.remove_component("instrumental")
    
    assert not result.is_valid
    assert "Cannot remove required component" in result.errors[0]

def test_validate_structure(test_dataset):
    """Test directory structure validation."""
    schema = DatasetComponentSchema.create(test_dataset)
    result = schema.validate()
    
    assert result.is_valid
    assert "directory_structure" in result.stats
    
    # Create a test track with components
    track_path = test_dataset / "Artist1" / "Album1"
    track_path.mkdir(parents=True)
    (track_path / "track1_instrumental.mp3").touch()
    (track_path / "track1_vocals.mp3").touch()
    
    # Add components to schema
    schema.add_component("instrumental", "*_instrumental.mp3")
    schema.add_component("vocals", "*_vocals.mp3")
    
    result = schema.validate()
    assert result.is_valid

def test_cd_structure(test_dataset):
    """Test CD directory structure validation."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Create valid CD structure
    track_path = test_dataset / "Artist1" / "Album1" / "CD1"
    track_path.mkdir(parents=True)
    (track_path / "track1_instrumental.mp3").touch()
    
    # Add component to schema
    schema.add_component("instrumental", "*_instrumental.mp3")
    
    result = schema.validate()
    assert result.is_valid

def test_discover_schema(test_dataset):
    """Test automatic schema discovery."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Clean up any existing test data
    if test_dataset.exists():
        shutil.rmtree(test_dataset)
    test_dataset.mkdir(parents=True)
    
    # Create test structure with special characters
    special_char_albums = [
        "Artist#1/Album@Special-2023 [#1]",
        "Artist$2/Album&Features^2 (Deluxe*)",
        "Artist~3/Album!Remix=2023+",
        "Artist-4/Album`with~Symbols-%"
    ]
    
    # Create album directories and test files
    for album_path in special_char_albums:
        album_dir = test_dataset / album_path
        album_dir.mkdir(parents=True, exist_ok=True)
        
        # Create test files with special characters
        base_names = [
            "01.Track#1with@symbols",
            "02.Track$2with^special",
            "03.Track&3with*chars",
            "04.Track-4with~signs"
        ]
        
        for base_name in base_names:
            # Create various component files
            (album_dir / f"{base_name}_instrumental.mp3").touch()
            (album_dir / f"{base_name}_vocals_noreverb.mp3").touch()
            (album_dir / f"{base_name}.mir.json").touch()
            (album_dir / f"{base_name}_vocals_stretched_120bpm_section1.mp3").touch()
            (album_dir / f"{base_name}_vocals_stretched_120bpm_section2.mp3").touch()
    
    # Run discovery
    result = schema.discover_schema()
    
    assert result.is_valid
    assert "instrumental.mp3" in schema.schema["components"]
    assert "vocals_noreverb.mp3" in schema.schema["components"]
    assert "mir.json" in schema.schema["components"]
    assert "vocals_stretched_120bpm_section*.mp3" in schema.schema["components"]
    
    # Check component properties
    assert schema.schema["components"]["instrumental.mp3"]["pattern"] == "*_instrumental.mp3"
    assert not schema.schema["components"]["instrumental.mp3"]["multiple"]
    
    assert schema.schema["components"]["vocals_noreverb.mp3"]["pattern"] == "*_vocals_noreverb.mp3"
    assert not schema.schema["components"]["vocals_noreverb.mp3"]["multiple"]
    
    # Check statistics
    assert result.stats["components"]["instrumental.mp3"]["file_count"] == 16  # 4 albums * 4 tracks
    assert result.stats["components"]["instrumental.mp3"]["track_coverage"] == 1.0
    assert not result.stats["components"]["instrumental.mp3"]["has_sections"]
    
    # Check that files with special characters were processed correctly
    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["file_count"] == 32  # 2 sections * 16 tracks
    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["multiple"]

def test_discover_schema_real_album():
    """Test schema discovery with a real album."""
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_RU/Vocal_Dereverb")
    album_to_analyze = ["7Б/Молодые ветра [2001]"]
    
    print("\n=== Starting test_discover_schema_real_album ===")
    print(f"Dataset path: {dataset_path}")
    print(f"Dataset path exists: {dataset_path.exists()}")
    print(f"Dataset path is dir: {dataset_path.is_dir()}")
    print(f"Album to analyze: {album_to_analyze}")
    
    # Create schema for the dataset but analyze just the specified album
    schema = DatasetComponentSchema(dataset_path)
    result = schema.discover_schema(folders=album_to_analyze)
    
    # Print debug info
    print("\nDiscovery result:")
    schema.parse_real_folder_and_report(dataset_path / album_to_analyze[0])
    print(f"Result valid: {result.is_valid}")
    print(f"Result stats: {result.stats}")
    print(f"Schema components: {schema.schema['components']}")
    
    # Verify the result is valid
    assert result.is_valid, "Schema discovery failed"
    
    # Verify all expected components are present
    components = schema.schema["components"]
    expected_components = {
        "instrumental.mp3", 
        "vocals_noreverb.mp3",
        "vocals_noreverb.json",
        "mir.json", 
        "caption.txt",
        "vocals_stretched_120bpm_section*.mp3",
        "vocals_stretched_120bpm_section*.json"
    }
    
    found_components = set(components.keys())
    print("\nComponent comparison:")
    print(f"Found: {found_components}")
    print(f"Expected: {expected_components}")
    
    assert found_components == expected_components, \
        f"Missing components. Found: {found_components}, Expected: {expected_components}"
    
    # Verify component configurations
    instrumental = components["instrumental.mp3"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats['components']["instrumental.mp3"]["track_coverage"] > 0

def test_validate_schema_different_album():
    """Test validating the schema against a different album."""
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_RU/Vocal_Dereverb")

    print("\nStep 1: Discovering schema from reference album")
    print("Album: 7Б/Молодые ветра [2001]")

    # First discover schema from one album
    schema = DatasetComponentSchema(dataset_path)
    discovery_result = schema.discover_schema(folders=["7Б/Молодые ветра [2001]"])
    assert discovery_result.is_valid, "Schema discovery failed"

    # Add vocals_noreverb component that exists in both albums
    schema.add_component(
        "vocals_noreverb",
        "*_vocals_noreverb.mp3",
        multiple=False
    )

    print("\nStep 2: Validating schema against different album")
    print("Album: 7Б/Моя любовь [2007]")

    # Now validate against a different album from the same artist
    validation_result = schema.validate_against_data(dataset_path / "7Б/Моя любовь [2007]")

    # Print validation results in a more organized way
    print("\nValidation Summary:")
    print(f"Total files found: {validation_result.stats['total_files']}")
    print(f"Files matched to components: {validation_result.stats['matched_files']}")
    print(f"Files not matching any component: {validation_result.stats['unmatched_files']}")

    if validation_result.errors:
        print("\nErrors Found:")
        for error in validation_result.errors:
            print(f"  - {error}")

    if validation_result.warnings:
        print("\nWarnings:")
        for warning in validation_result.warnings:
            print(f"  - {warning}")

    print("\nComponent Coverage:")
    for component, stats in validation_result.stats["component_coverage"].items():
        print(f"\n{component}:")
        print(f"  Matched files: {stats['matched']}")
        if stats['unmatched'] > 0:
            print(f"  ⚠️  Unmatched files: {stats['unmatched']}")
        else:
            print(f"  ✓ All files matched")

    # Verify that validation passes and all files are matched to components
    assert validation_result.is_valid, "Schema validation failed"
    assert validation_result.stats["unmatched_files"] == 0, "Found files not matching any component"
    
    # Verify that different components can have different coverage
    coverage = validation_result.stats["component_coverage"]
    assert any(stats["matched"] != coverage["instrumental.mp3"]["matched"] 
              for component, stats in coverage.items()), "Expected different components to have different coverage"

def test_discover_schema_with_cd_album():
    """Test schema discovery with a multi-CD album."""
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_RU/Vocal_Dereverb")
    
    # Album with CDs to analyze
    album_to_analyze = ["Alai Oli/Последний из ушедших [2022]"]
    
    print("\nAnalyzing multi-CD album:")
    print(f"- {album_to_analyze[0]}")
    
    # Create schema for the dataset but analyze just the specified album
    schema = DatasetComponentSchema(dataset_path)
    result = schema.discover_schema(folders=album_to_analyze)
    
    assert result.is_valid
    
    # Print discovered schema in a more readable format
    print("\nDiscovered Components:")
    for name, config in schema.schema["components"].items():
        print(f"\n{name}:")
        print(f"  Pattern: {config['pattern']}")
        print(f"  Multiple: {config['multiple']}")
        if "description" in config:
            print(f"  Description: {config['description']}")
            
        # Print corresponding statistics
        if name in result.stats["components"]:
            stats = result.stats["components"][name]
            print("\n  Statistics:")
            print(f"    Files found: {stats['file_count']}")
            print(f"    Track coverage: {stats['track_coverage']*100:.1f}%")
            print(f"    Unique tracks: {stats['unique_tracks']}")
            print(f"    Has sections: {stats['has_sections']}")
    
    # Check discovered components
    components = schema.schema["components"]
    assert "instrumental.mp3" in components
    assert "vocals_noreverb.mp3" in components
    assert "vocals_stretched_120bpm_section*.mp3" in components
    assert "mir.json" in components
    
    # Check instrumental component
    instrumental = components["instrumental.mp3"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats["components"]["instrumental.mp3"]["track_coverage"] > 0.9
    
    # Check vocals component
    vocals = components["vocals_noreverb.mp3"]
    assert vocals["pattern"] == "*_vocals_noreverb.mp3"
    assert vocals["multiple"] is False
    assert result.stats["components"]["vocals_noreverb.mp3"]["track_coverage"] > 0.9
    
    # Check sections component
    sections = components["vocals_stretched_120bpm_section*.mp3"]
    assert sections["multiple"] is True
    assert sections["pattern"] == "*_vocals_stretched_120bpm_section*.mp3"
    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["track_coverage"] > 0.9

def test_add_component(tmp_path):
    """Test adding a new component to the schema."""
    schema = DatasetComponentSchema(tmp_path)
    
    # Add instrumental component
    result = schema.add_component(
        "instrumental",
        "*_instrumental.mp3",
        multiple=False
    )
    assert result.is_valid
    assert "instrumental" in schema.schema["components"]
    assert schema.schema["components"]["instrumental"]["pattern"] == "*_instrumental.mp3"
    assert schema.schema["components"]["instrumental"]["multiple"] is False

def test_remove_component(tmp_path):
    """Test removing a component from the schema."""
    schema = DatasetComponentSchema(tmp_path)
    
    # Add component first
    schema.add_component("vocals", "*_vocals.mp3", multiple=False)
    assert "vocals" in schema.schema["components"]
    
    # Remove it
    result = schema.remove_component("vocals")
    assert result.is_valid
    assert "vocals" not in schema.schema["components"]

def test_webdav_sync_with_special_chars():
    """Test WebDAV sync with special characters in album and file names."""
    import socket
    import time
    from urllib.error import URLError

    # Check if WebDAV server is running on port 2222
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('localhost', 2222))
        webdav_available = True
    except (ConnectionRefusedError, socket.error):
        webdav_available = False
    finally:
        sock.close()

    if not webdav_available:
        print("\nWebDAV server not found on port 2222!")
        print("Please start a WebDAV server with the following configuration:")
        print("- Port: 2222")
        print("- Root directory: ./test_dataset_folder")
        print("- No authentication required for testing")
        pytest.skip("WebDAV server not available")
        return

    # Create source dataset with special characters
    source_path = Path("test_dataset_folder")
    if source_path.exists():
        # Only remove contents, not the directory itself
        for item in source_path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir() and item.name != '.blackbird':
                shutil.rmtree(item)
    else:
        source_path.mkdir(parents=True)

    # Initialize schema in source dataset
    source_schema = DatasetComponentSchema.create(source_path)
    source_schema.add_component("instrumental.mp3", "*_instrumental.mp3")
    source_schema.add_component("vocals_noreverb.mp3", "*_vocals_noreverb.mp3")
    source_schema.add_component("mir.json", "*.mir.json")
    source_schema.add_component("vocals_stretched_120bpm_section*.mp3", "*_vocals_stretched_*.mp3", multiple=True)
    source_schema.save()  # Save the schema after adding components

    # Create test structure with special characters (same as in test_discover_schema)
    special_char_albums = [
        "Artist#1/Album@Special_2023 [#1]",
        "Artist$2/Album&Features^2 (Deluxe*)",
        "Artist~3/Album!Remix=2023+",
        "Artist-4/Album`with~Symbols_%"
    ]

    # Create album directories and test files in source
    for album_path in special_char_albums:
        album_dir = source_path / album_path
        album_dir.mkdir(parents=True, exist_ok=True)

        base_names = [
            "01.Track#1_with@symbols",
            "02.Track$2_with^special",
            "03.Track&3_with*chars",
            "04.Track-4_with~signs"
        ]

        for base_name in base_names:
            # Create test files
            for file_name in [
                f"{base_name}_instrumental.mp3",
                f"{base_name}_vocals_noreverb.mp3",
                f"{base_name}.mir.json",
                f"{base_name}_vocals_stretched_120bpm_section1.mp3",
                f"{base_name}_vocals_stretched_120bpm_section2.mp3"
            ]:
                file_path = album_dir / file_name
                file_path.touch()
                # Write some content to make it a real file
                file_path.write_bytes(b"Test content for WebDAV sync")

    # Create Dataset instance to build and save the index
    source_dataset = Dataset(source_path)
    source_dataset.rebuild_index()  # This will build and save the index
    source_dataset.index.save(source_path / '.blackbird' / 'index.pickle')  # Explicitly save the index

    # Create destination for sync test
    dest_path = Path("test_dataset_folder_sync")
    if dest_path.exists():
        shutil.rmtree(dest_path)
    dest_path.mkdir(parents=True)

    # Initialize schema in destination
    dest_schema = DatasetComponentSchema.create(dest_path)

    try:
        # Configure WebDAV client and clone
        from blackbird.sync import clone_dataset
        result = clone_dataset(
            source_url="webdav://localhost:2222",
            destination=dest_path,
            components=["instrumental.mp3", "vocals_noreverb.mp3", "mir.json"]
        )

        # Verify the sync worked
        assert result.total_files > 0, "No files were synced"
        assert result.failed_files == 0, f"{result.failed_files} files failed to sync"

        # Check that files with special characters were synced correctly
        for album_path in special_char_albums:
            album_dir = dest_path / album_path
            assert album_dir.exists(), f"Album directory not synced: {album_path}"

            # Check a few sample files
            test_file = album_dir / "01.Track#1_with@symbols_instrumental.mp3"
            assert test_file.exists(), f"Test file not synced: {test_file}"
            assert test_file.read_bytes() == b"Test content for WebDAV sync"

    except Exception as e:
        print(f"\nError during WebDAV sync test: {e}")
        raise

    finally:
        # Only clean up the destination directory
        if dest_path.exists():
            shutil.rmtree(dest_path)
