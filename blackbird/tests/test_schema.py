import pytest
from pathlib import Path
import shutil
import json
from blackbird.schema import DatasetComponentSchema
from blackbird.dataset import Dataset


@pytest.fixture
def test_dataset(tmp_path):
    """Create a test dataset structure."""
    dataset_root = tmp_path / "test_dataset"
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
    schema = DatasetComponentSchema.create(test_dataset)

    result = schema.add_component(
        "vocals_noreverb",
        "*_vocals_noreverb.mp3",
        multiple=False
    )

    assert result.is_valid, "Validation failed"
    assert "vocals_noreverb" in schema.schema["components"]
    assert schema.schema["components"]["vocals_noreverb"]["pattern"] == "*_vocals_noreverb.mp3"
    assert schema.schema["components"]["vocals_noreverb"]["multiple"] is False


def test_remove_component(test_dataset):
    """Test removing a component."""
    schema = DatasetComponentSchema(test_dataset)

    # Add and then remove a component
    schema.add_component("vocals", "*_vocals.mp3", multiple=False)
    assert "vocals" in schema.schema["components"]

    result = schema.remove_component("vocals")
    assert result.is_valid
    assert "vocals" not in schema.schema["components"]


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

    # Clean up and recreate (to start fresh)
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

        base_names = [
            "01.Track#1with@symbols",
            "02.Track$2with^special",
            "03.Track&3with*chars",
            "04.Track-4with~signs"
        ]

        for base_name in base_names:
            (album_dir / f"{base_name}_instrumental.mp3").touch()
            (album_dir / f"{base_name}_vocals_noreverb.mp3").touch()
            (album_dir / f"{base_name}.mir.json").touch()
            (album_dir / f"{base_name}_vocals_stretched_120bpm_section1.mp3").touch()
            (album_dir / f"{base_name}_vocals_stretched_120bpm_section2.mp3").touch()

    result = schema.discover_schema()

    assert result.is_valid
    assert "instrumental.mp3" in schema.schema["components"]
    assert "vocals_noreverb.mp3" in schema.schema["components"]
    assert "mir.json" in schema.schema["components"]
    assert "vocals_stretched_120bpm_section*.mp3" in schema.schema["components"]

    assert schema.schema["components"]["instrumental.mp3"]["pattern"] == "*_instrumental.mp3"
    assert not schema.schema["components"]["instrumental.mp3"]["multiple"]

    assert schema.schema["components"]["vocals_noreverb.mp3"]["pattern"] == "*_vocals_noreverb.mp3"
    assert not schema.schema["components"]["vocals_noreverb.mp3"]["multiple"]

    assert result.stats["components"]["instrumental.mp3"]["file_count"] == 16  # 4 albums * 4 tracks
    assert result.stats["components"]["instrumental.mp3"]["track_coverage"] == 1.0
    assert not result.stats["components"]["instrumental.mp3"]["has_sections"]

    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["file_count"] == 32  # 2 sections * 16 tracks
    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["multiple"]


def _create_album_files(album_dir, track_bases, components):
    """Helper to create track files for an album.

    Args:
        album_dir: Path to the album directory
        track_bases: list of track base names (e.g. ["01.Artist - Track1"])
        components: list of suffixes (e.g. ["_instrumental.mp3", ".mir.json"])
    """
    album_dir.mkdir(parents=True, exist_ok=True)
    for base in track_bases:
        for suffix in components:
            (album_dir / f"{base}{suffix}").touch()


def test_discover_schema_real_album(tmp_path):
    """Test schema discovery with a realistic album structure."""
    dataset_path = tmp_path / "dataset"
    dataset_path.mkdir()

    # Create a realistic album with all component types
    album_dir = dataset_path / "7Б" / "Молодые ветра [2001]"
    track_bases = [
        "01.7Б - Молодые ветра",
        "02.7Б - Песня для двоих",
        "03.7Б - Летим с ветром",
    ]
    suffixes = [
        "_instrumental.mp3",
        "_vocals_noreverb.mp3",
        "_vocals_noreverb.json",
        ".mir.json",
        "_caption.txt",
        "_vocals_stretched_120bpm_section1.mp3",
        "_vocals_stretched_120bpm_section2.mp3",
        "_vocals_stretched_120bpm_section1.json",
        "_vocals_stretched_120bpm_section2.json",
        ".mp3",
    ]
    _create_album_files(album_dir, track_bases, suffixes)

    schema = DatasetComponentSchema(dataset_path)
    result = schema.discover_schema(folders=["7Б/Молодые ветра [2001]"])

    assert result.is_valid, "Schema discovery failed"

    components = schema.schema["components"]
    expected_components = {
        "instrumental.mp3",
        "vocals_noreverb.mp3",
        "vocals_noreverb.json",
        "mir.json",
        "caption.txt",
        "vocals_stretched_120bpm_section*.mp3",
        "vocals_stretched_120bpm_section*.json",
        "mp3"
    }

    found_components = set(components.keys())
    assert found_components == expected_components, \
        f"Missing components. Found: {found_components}, Expected: {expected_components}"

    instrumental = components["instrumental.mp3"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats['components']["instrumental.mp3"]["track_coverage"] > 0


def test_validate_schema_different_album(tmp_path):
    """Test validating a discovered schema against a different album."""
    dataset_path = tmp_path / "dataset"
    dataset_path.mkdir()

    # Use suffixes without plain .mp3 to avoid catch-all pattern conflicts
    suffixes = [
        "_instrumental.mp3",
        "_vocals_noreverb.mp3",
        "_vocals_noreverb.json",
        ".mir.json",
        "_caption.txt",
        "_vocals_stretched_120bpm_section1.mp3",
        "_vocals_stretched_120bpm_section2.mp3",
        "_vocals_stretched_120bpm_section1.json",
        "_vocals_stretched_120bpm_section2.json",
    ]

    # Create reference album for discovery
    ref_album = dataset_path / "7Б" / "Молодые ветра [2001]"
    track_bases_ref = [
        "01.7Б - Молодые ветра",
        "02.7Б - Песня для двоих",
    ]
    _create_album_files(ref_album, track_bases_ref, suffixes)

    # Create a second album for validation
    val_album = dataset_path / "7Б" / "Моя любовь [2007]"
    track_bases_val = [
        "01.7Б - Моя любовь",
        "02.7Б - Если я",
        "03.7Б - Утро",
    ]
    _create_album_files(val_album, track_bases_val, suffixes)

    # Discover schema from reference album
    schema = DatasetComponentSchema(dataset_path)
    discovery_result = schema.discover_schema(folders=["7Б/Молодые ветра [2001]"])
    assert discovery_result.is_valid, "Schema discovery failed"

    # Validate against the second album
    validation_result = schema.validate_against_data(dataset_path / "7Б/Моя любовь [2007]")

    assert validation_result.is_valid, "Schema validation failed"
    assert validation_result.stats["unmatched_files"] == 0, "Found files not matching any component"


def test_discover_schema_with_cd_album(tmp_path):
    """Test schema discovery with a multi-CD album."""
    dataset_path = tmp_path / "dataset"
    dataset_path.mkdir()

    # Create multi-CD album
    album_base = dataset_path / "Alai Oli" / "Последний из ушедших [2022]"
    track_bases_cd1 = [
        "01.Alai Oli - Последний",
        "02.Alai Oli - Ушедший",
    ]
    track_bases_cd2 = [
        "01.Alai Oli - Бонус трек 1",
        "02.Alai Oli - Бонус трек 2",
    ]
    suffixes = [
        "_instrumental.mp3",
        "_vocals_noreverb.mp3",
        "_vocals_stretched_120bpm_section1.mp3",
        "_vocals_stretched_120bpm_section2.mp3",
        ".mir.json",
        ".mp3",
    ]
    _create_album_files(album_base / "CD1", track_bases_cd1, suffixes)
    _create_album_files(album_base / "CD2", track_bases_cd2, suffixes)

    schema = DatasetComponentSchema(dataset_path)
    result = schema.discover_schema(folders=["Alai Oli/Последний из ушедших [2022]"])

    assert result.is_valid

    components = schema.schema["components"]
    assert "instrumental.mp3" in components
    assert "vocals_noreverb.mp3" in components
    assert "vocals_stretched_120bpm_section*.mp3" in components
    assert "mir.json" in components
    assert "mp3" in components

    instrumental = components["instrumental.mp3"]
    assert instrumental["pattern"] == "*_instrumental.mp3"
    assert instrumental["multiple"] is False
    assert result.stats["components"]["instrumental.mp3"]["track_coverage"] > 0.9

    vocals = components["vocals_noreverb.mp3"]
    assert vocals["pattern"] == "*_vocals_noreverb.mp3"
    assert vocals["multiple"] is False
    assert result.stats["components"]["vocals_noreverb.mp3"]["track_coverage"] > 0.9

    sections = components["vocals_stretched_120bpm_section*.mp3"]
    assert sections["multiple"] is True
    assert sections["pattern"] == "*_vocals_stretched_120bpm_section*.mp3"
    assert result.stats["components"]["vocals_stretched_120bpm_section*.mp3"]["track_coverage"] > 0.9


def test_webdav_sync_with_special_chars(tmp_path):
    """Test WebDAV sync with special characters in album and file names."""
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('localhost', 2222))
        webdav_available = True
    except (ConnectionRefusedError, socket.error):
        webdav_available = False
    finally:
        sock.close()

    if not webdav_available:
        pytest.skip("WebDAV server not available on port 2222")

    # Create source dataset in tmp_path
    source_path = tmp_path / "test_dataset_folder"
    source_path.mkdir(parents=True)

    source_schema = DatasetComponentSchema.create(source_path)
    source_schema.add_component("instrumental.mp3", "*_instrumental.mp3")
    source_schema.add_component("vocals_noreverb.mp3", "*_vocals_noreverb.mp3")
    source_schema.add_component("mir.json", "*.mir.json")
    source_schema.add_component("vocals_stretched_120bpm_section*.mp3", "*_vocals_stretched_*.mp3", multiple=True)
    source_schema.save()

    special_char_albums = [
        "Artist#1/Album@Special_2023 [#1]",
        "Artist$2/Album&Features^2 (Deluxe*)",
        "Artist~3/Album!Remix=2023+",
        "Artist-4/Album`with~Symbols_%"
    ]

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
            for file_name in [
                f"{base_name}_instrumental.mp3",
                f"{base_name}_vocals_noreverb.mp3",
                f"{base_name}.mir.json",
                f"{base_name}_vocals_stretched_120bpm_section1.mp3",
                f"{base_name}_vocals_stretched_120bpm_section2.mp3"
            ]:
                (album_dir / file_name).write_bytes(b"Test content for WebDAV sync")

    source_dataset = Dataset(source_path)
    source_dataset.rebuild_index()
    source_dataset.index.save(source_path / '.blackbird' / 'index.pickle')

    # Create destination in tmp_path
    dest_path = tmp_path / "test_dataset_folder_sync"
    dest_path.mkdir(parents=True)

    DatasetComponentSchema.create(dest_path)

    from blackbird.sync import clone_dataset
    result = clone_dataset(
        source_url="webdav://localhost:2222",
        destination=dest_path,
        components=["instrumental.mp3", "vocals_noreverb.mp3", "mir.json"]
    )

    assert result.total_files > 0, "No files were synced"
    assert result.failed_files == 0, f"{result.failed_files} files failed to sync"

    for album_path in special_char_albums:
        album_dir = dest_path / album_path
        assert album_dir.exists(), f"Album directory not synced: {album_path}"

        test_file = album_dir / "01.Track#1_with@symbols_instrumental.mp3"
        assert test_file.exists(), f"Test file not synced: {test_file}"
        assert test_file.read_bytes() == b"Test content for WebDAV sync"
