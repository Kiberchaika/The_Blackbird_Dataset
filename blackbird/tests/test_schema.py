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
