import pytest
from pathlib import Path
import shutil
import json
from blackbird.schema import DatasetComponentSchema

@pytest.fixture
def test_dataset(tmp_path):
    """Create a test dataset structure."""
    dataset_root = tmp_path / "test_dataset"
    
    # Create basic structure
    (dataset_root / "Artist1" / "Album1").mkdir(parents=True)
    (dataset_root / "Artist2" / "Album1" / "CD1").mkdir(parents=True)
    (dataset_root / "Artist2" / "Album1" / "CD2").mkdir(parents=True)
    
    # Create test files
    (dataset_root / "Artist1" / "Album1" / "track1_instrumental.mp3").touch()
    (dataset_root / "Artist1" / "Album1" / "track1_vocals_noreverb.mp3").touch()
    (dataset_root / "Artist1" / "Album1" / "track1.mir.json").touch()
    
    (dataset_root / "Artist2" / "Album1" / "CD1" / "track1_instrumental.mp3").touch()
    (dataset_root / "Artist2" / "Album1" / "CD2" / "track1_instrumental.mp3").touch()
    
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
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add vocals component
    result = schema.add_component(
        "vocals",
        pattern="*_vocals_noreverb.mp3",
        required=False
    )
    
    assert result.is_valid
    assert result.stats["matched_files"] == 1
    assert "vocals" in schema.schema["components"]
    
    # Try adding invalid component
    result = schema.add_component(
        "invalid name",
        pattern="*.txt"
    )
    
    assert not result.is_valid
    assert "Invalid component name" in result.errors[0]

def test_remove_component(test_dataset):
    """Test removing a component."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add and then remove a component
    schema.add_component("vocals", "*_vocals_noreverb.mp3")
    result = schema.remove_component("vocals")
    
    assert result.is_valid
    assert "vocals" not in schema.schema["components"]
    
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
    
    # Add some test files
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
    assert "vocals" in schema.schema["components"]
    assert "mir" in schema.schema["components"]
    
    # Check component properties
    assert schema.schema["components"]["instrumental"]["required"] is True
    assert schema.schema["components"]["instrumental"]["pattern"] == "*_instrumental.mp3"
    assert not schema.schema["components"]["instrumental"]["multiple"]
    
    assert schema.schema["components"]["vocals"]["required"] is False
    assert schema.schema["components"]["vocals"]["pattern"] == "*_vocals_noreverb.mp3"
    
    # Check statistics
    assert result.stats["instrumental"]["file_count"] == 2
    assert result.stats["instrumental"]["track_coverage"] == 1.0
    assert not result.stats["instrumental"]["has_multiple"]

def test_discover_schema_with_multiple(test_dataset):
    """Test schema discovery with multiple files per component."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add test files with multiple sections
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_section1.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_section2.mp3").touch()
    
    result = schema.discover_schema()
    
    assert result.is_valid
    assert "section" in schema.schema["components"]
    assert schema.schema["components"]["section"]["multiple"] is True
    assert result.stats["section"]["has_multiple"] is True

def test_validate_against_data(test_dataset):
    """Test schema validation against dataset."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add test files
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_vocals_noreverb.mp3").touch()
    (test_dataset / "Artist1/Album1/track2_instrumental.mp3").touch()
    # track2 missing vocals (optional)
    
    # Add vocals component
    schema.add_component(
        "vocals",
        pattern="*_vocals_noreverb.mp3",
        required=False
    )
    
    # Validate
    result = schema.validate_against_data()
    
    assert result.is_valid
    assert result.stats["total_files"] == 3
    assert result.stats["matched_files"] == 3
    assert result.stats["unmatched_files"] == 0
    assert result.stats["component_coverage"]["instrumental"]["matched"] == 2
    assert result.stats["component_coverage"]["vocals"]["matched"] == 1

def test_validate_missing_required(test_dataset):
    """Test validation with missing required component."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add test files with missing instrumental
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_vocals_noreverb.mp3").touch()
    (test_dataset / "Artist1/Album1/track2_vocals_noreverb.mp3").touch()
    # track2 missing instrumental (required)
    
    result = schema.validate_against_data()
    
    assert not result.is_valid
    assert any("Required component 'instrumental' missing" in error for error in result.errors)

def test_validate_pattern_collision(test_dataset):
    """Test validation with pattern collision between components."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add component with overlapping pattern
    schema.add_component(
        "overlap",
        pattern="*_instrumental.mp3",  # Same as instrumental pattern
        required=False
    )
    
    # Add test file
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    
    result = schema.validate_against_data()
    
    assert not result.is_valid
    assert any("Pattern collision between" in error for error in result.errors)

def test_validate_multiple_constraint(test_dataset):
    """Test validation of multiple files constraint."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add test files with multiple instrumentals
    (test_dataset / "Artist1/Album1/track1_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1/track1_instrumental_v2.mp3").touch()
    
    result = schema.validate_against_data()
    
    assert not result.is_valid
    assert any("has multiple files for tracks" in error for error in result.errors)

def test_discover_schema_with_numbered_sections(test_dataset):
    """Test schema discovery with numbered section files."""
    schema = DatasetComponentSchema.create(test_dataset)
    
    # Add test files with numbered sections
    base_name = "09.Центр - Навсегда (Всё наше)"
    (test_dataset / "Artist1/Album1" / f"{base_name}_instrumental.mp3").touch()
    (test_dataset / "Artist1/Album1" / f"{base_name}_vocals_noreverb.mp3").touch()
    (test_dataset / "Artist1/Album1" / f"{base_name}_vocals_noreverb.json").touch()
    
    # Add multiple numbered sections
    for i in [2, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14]:
        (test_dataset / "Artist1/Album1" / f"{base_name}_vocals_stretched_120bpm_section{i}.mp3").touch()
        (test_dataset / "Artist1/Album1" / f"{base_name}_vocals_stretched_120bpm_section{i}.json").touch()
    
    result = schema.discover_schema()
    
    assert result.is_valid
    
    # Check discovered components
    components = schema.schema["components"]
    assert "instrumental" in components
    assert "vocals_noreverb" in components
    assert "vocals_stretched_120bpm_section" in components
    
    # Verify section component properties
    section_comp = components["vocals_stretched_120bpm_section"]
    assert section_comp["multiple"] is True
    assert section_comp["required"] is False
    assert "[0-9]+" in section_comp["pattern"]
    
    # Check statistics
    section_stats = result.stats["vocals_stretched_120bpm_section"]
    assert section_stats["file_count"] == 22  # 11 sections * 2 files each
    assert section_stats["has_multiple"] is True
    assert section_stats["unique_tracks"] == 1  # All sections belong to same track

def test_discover_schema_real_album():
    """Test schema discovery with a single album."""
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    
    # Single album to analyze
    album_to_analyze = ["7Б/Молодые ветра [2001]"]
    
    print("\nAnalyzing album:")
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
    
    print("\nSync Configuration:")
    print(json.dumps(schema.schema["sync"], indent=2))
    
    # Check discovered components
    components = schema.schema["components"]
    assert "instrumental_audio" in components
    assert "vocals_noreverb_lyrics" in components
    assert "vocals_stretched_audio" in components
    assert "mir" in components
    
    # Check instrumental audio component
    instrumental = components["instrumental_audio"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False  # Each track has exactly one instrumental file
    assert result.stats["instrumental_audio"]["track_coverage"] >= 0.95  # Should be close to 100%
    assert result.stats["instrumental_audio"]["max_files_per_track"] == 1
    
    # Check vocals component
    vocals = components["vocals_noreverb_lyrics"]
    assert vocals["pattern"] == "*_vocals_noreverb.json"
    assert vocals["multiple"] is False  # Each track has exactly one lyrics file
    assert result.stats["vocals_noreverb_lyrics"]["track_coverage"] >= 0.95  # Should be close to 100%
    assert result.stats["vocals_noreverb_lyrics"]["max_files_per_track"] == 1
    
    # Check sections component
    sections = components["vocals_stretched_audio"]
    assert sections["multiple"] is True  # Multiple stretched files per track
    assert sections["pattern"] == "*_vocals_stretched.mp3"
    assert result.stats["vocals_stretched_audio"]["track_coverage"] >= 0.95  # Should be close to 100%
    assert result.stats["vocals_stretched_audio"]["max_files_per_track"] >= 5  # At least 5 files per track
    assert result.stats["vocals_stretched_audio"]["min_files_per_track"] >= 5  # At least 5 files per track

    # Check sections component with section pattern
    sections_with_section = components["vocals_stretched_audio_section"]  # Note: _audio_section, not _section_audio
    assert sections_with_section["multiple"] is True
    assert sections_with_section["pattern"] == "*_vocals_stretched_audio_*section*.mp3"
    assert result.stats["vocals_stretched_audio_section"]["track_coverage"] >= 0.95  # Should be close to 100%
    assert result.stats["vocals_stretched_audio_section"]["max_files_per_track"] >= 5
    assert result.stats["vocals_stretched_audio_section"]["min_files_per_track"] >= 5
    
    # Check MIR component
    mir = components["mir"]
    assert mir["pattern"] == "*.mir.json"
    assert mir["multiple"] is False  # Each track has exactly one MIR file
    assert result.stats["mir"]["track_coverage"] >= 0.95  # Should be close to 100%
    assert result.stats["mir"]["max_files_per_track"] == 1

def test_validate_schema_different_album():
    """Test validating the schema against a different album."""
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    
    print("\nStep 1: Discovering schema from reference album")
    print("Album: Центр/Дитятя [1988]")
    
    # First discover schema from one album
    schema = DatasetComponentSchema(dataset_path)
    discovery_result = schema.discover_schema(folders=["Центр/Дитятя [1988]"])
    assert discovery_result.is_valid, "Schema discovery failed"
    
    print("\nStep 2: Validating schema against different album")
    print("Album: Центр/Сделано в Париже [1989]")
    
    # Now validate against a different album
    validation_result = schema.validate_against_data(dataset_path / "Центр/Сделано в Париже [1989]")
    
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

def test_discover_schema_with_cd_album():
    """Test schema discovery with a multi-CD album."""
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    
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
    assert "mir" in components
    
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
