import pytest
from pathlib import Path
import shutil
import json
from blackbird.schema import DatasetComponentSchema
import os

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
    assert "instrumental" in data["components"]
    assert data["components"]["instrumental"]["required"] is True

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
    
    # Test invalid structure
    invalid_path = test_dataset / "Invalid" / "Path" / "Extra" / "Level" / "track_instrumental.mp3"
    invalid_path.parent.mkdir(parents=True)
    invalid_path.touch()
    
    result = schema.validate()
    assert not result.is_valid
    assert any("Path too deep" in error for error in result.errors)

def test_cd_structure(test_dataset):
    """Test CD directory structure validation."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Valid CD structure
    result = schema.validate()
    assert result.is_valid
    
    # Invalid CD name
    invalid_cd = test_dataset / "Artist3" / "Album1" / "Disc1" / "track_instrumental.mp3"
    invalid_cd.parent.mkdir(parents=True)
    invalid_cd.touch()
    
    result = schema.validate()
    assert not result.is_valid
    assert any("Invalid CD directory format" in error for error in result.errors)

def test_discover_schema(test_dataset):
    """Test automatic schema discovery."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Clean up any existing test data
    if test_dataset.exists():
        shutil.rmtree(test_dataset)
    test_dataset.mkdir(parents=True)
    (test_dataset / "Artist1" / "Album1").mkdir(parents=True)
    
    # Add test files
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_vocals_noreverb.mp3").touch()
    (test_dataset / "Artist1/Album1/track1.mir.json").touch()
    (test_dataset / "Artist1/Album1/track2_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track2_vocals_noreverb.mp3").touch()
    (test_dataset / "Artist1/Album1/track2.mir.json").touch()
    
    # Run discovery
    result = schema.discover_schema()
    
    assert result.is_valid
    assert "instrumental" in schema.schema["components"]
    assert "vocals_noreverb" in schema.schema["components"]
    assert "mir.json" in schema.schema["components"]
    
    # Check component properties
    assert schema.schema["components"]["instrumental"]["pattern"] == "*_instrumental.mp3"
    assert not schema.schema["components"]["instrumental"]["multiple"]
    
    assert schema.schema["components"]["vocals_noreverb"]["required"] is False
    assert schema.schema["components"]["vocals_noreverb"]["pattern"] == "*_vocals_noreverb.mp3"
    
    # Check statistics
    assert result.stats["instrumental"]["file_count"] == 2
    assert result.stats["instrumental"]["track_coverage"] == 1.0
    assert not result.stats["instrumental"]["has_multiple"]

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
        "instrumental", "vocals_noreverb", "mir.json", "caption",
        "vocals_stretched_120bpm_section*_json", "vocals_stretched_120bpm_section*_mp3"
    }
    assert set(components.keys()) == expected_components, \
        f"Missing components. Found: {set(components.keys())}, Expected: {expected_components}"
    
    # Verify component configurations
    instrumental = components["instrumental"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats['components']["instrumental"]["track_coverage"] == 1.0
    
    vocals = components["vocals_noreverb"]
    assert vocals["pattern"] == "*_vocals_noreverb.mp3"
    assert vocals["required"] is False
    assert vocals["multiple"] is False
    assert result.stats['components']["vocals_noreverb"]["track_coverage"] > 0
    
    mir = components["mir.json"]
    assert mir["pattern"] == "*.mir.json"
    assert mir["required"] is False
    assert mir["multiple"] is False
    assert result.stats['components']["mir.json"]["track_coverage"] > 0
    
    caption = components["caption"]
    assert caption["pattern"] == "*_caption.txt"
    assert caption["required"] is False
    assert caption["multiple"] is False
    
    stretched = components["vocals_stretched_section"]
    assert stretched["pattern"] == "*_vocals_stretched_120bpm_section*.mp3"
    assert stretched["required"] is False
    assert stretched["multiple"] is True
    assert result.stats['components']["vocals_stretched_120bpm_section*_json"]["has_sections"] is True
    
    

def test_validate_schema_different_album():
    """Test validating the schema against a different album."""
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_RU/Vocal_Dereverb")
    
    print("\nStep 1: Discovering schema from reference album")
    print("Album: 7Б/Молодые ветра [2001]")
    
    # First discover schema from one album
    schema = DatasetComponentSchema(dataset_path)
    discovery_result = schema.discover_schema(folders=["7Б/Молодые ветра [2001]"])
    assert discovery_result.is_valid, "Schema discovery failed"
    
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
    
    # Verify validation
    assert validation_result.is_valid, "Schema validation failed"
    assert validation_result.stats["total_files"] > 0, "No files found in validation album"
    assert validation_result.stats["unmatched_files"] == 0, "Found files not matching any component"
    
    # Verify component coverage
    for component, stats in validation_result.stats["component_coverage"].items():
        assert stats["matched"] > 0, f"No files matched for component {component}"
        assert stats["unmatched"] == 0, f"Found unmatched files for component {component}"

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
        if name in result.stats:
            stats = result.stats[name]
            print("\n  Statistics:")
            print(f"    Files found: {stats['file_count']}")
            print(f"    Track coverage: {stats['track_coverage']*100:.1f}%")
            print(f"    Unique tracks: {stats['unique_tracks']}")
            print(f"    Files per track: {stats['min_files_per_track']} to {stats['max_files_per_track']}")
            print(f"    Extensions: {', '.join(stats['extensions'])}")
            print(f"    Has sections: {stats['has_sections']}")
    
    print("\nDirectory Structure:")
    print(json.dumps(schema.schema["structure"], indent=2))
    
    # Check discovered components
    components = schema.schema["components"]
    assert "instrumental_audio" in components
    assert "vocals_noreverb_lyrics" in components
    assert "vocals_stretched_audio" in components
    assert "mir.json" in components
    
    # Check instrumental audio component
    instrumental = components["instrumental_audio"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats["instrumental_audio"]["track_coverage"] >= 0.95
    assert result.stats["instrumental_audio"]["max_files_per_track"] == 1
    
    # Check vocals component
    vocals = components["vocals_noreverb_lyrics"]
    assert vocals["pattern"] == "*_vocals_noreverb.json"
    assert vocals["multiple"] is False
    assert result.stats["vocals_noreverb_lyrics"]["track_coverage"] >= 0.95
    assert result.stats["vocals_noreverb_lyrics"]["max_files_per_track"] == 1
    
    # Check sections component
    sections = components["vocals_stretched_audio"]
    assert sections["multiple"] is True
    assert sections["pattern"] == "*_vocals_stretched.mp3"
    assert result.stats["vocals_stretched_audio"]["track_coverage"] >= 0.90  # Slightly lower threshold for CD albums
    
    # Verify CD structure is properly handled
    assert schema.schema["structure"]["artist_album_format"]["is_cd_optional"] is True
    assert schema.schema["structure"]["artist_album_format"]["cd_pattern"] == "CD\\d+"
    assert "?cd" in schema.schema["structure"]["artist_album_format"]["levels"]  # CD level is optional, so it's marked with ?

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
