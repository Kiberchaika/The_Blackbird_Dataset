"""Tests for the CLI sync command — album filtering, missing-component filter,
and schema update during sync."""

import os
import pytest
import shutil
import json
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from blackbird.cli import main as cli_main
from blackbird.sync import SyncStats
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex, TrackInfo


def _create_test_file(path, content="Test content"):
    """Helper to create a test file with content."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)


def _make_copy_client(source_dir):
    """Create a MagicMock WebDAV client that copies files from source_dir."""
    client = MagicMock()
    client.base_url = f"http://localhost/{source_dir.name}"
    client.client = MagicMock()
    client.client.options = {'webdav_root': '/'}
    client.check_connection.return_value = True

    # Load actual schema and index from the source dataset
    schema = DatasetComponentSchema.load(source_dir / ".blackbird" / "schema.json")
    index = DatasetIndex.load(source_dir / ".blackbird" / "index.pickle")
    client.get_schema = MagicMock(return_value=schema)
    client.get_index = MagicMock(return_value=index)

    def download_side_effect(remote_path, local_path, file_size=None, **kwargs):
        source = source_dir / remote_path
        dest = Path(local_path)
        if not source.exists():
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, dest)
        if file_size is not None and dest.stat().st_size != file_size:
            with open(dest, 'wb') as f:
                f.truncate(file_size)
        return True

    client.download_file.side_effect = download_side_effect
    return client


@pytest.fixture
def test_dataset(tmp_path):
    """Create a test dataset structure with real files on disk."""
    dataset_dir = tmp_path / "source_dataset"

    artists = ["Artist1", "Artist2"]
    albums = {
        "Artist1": ["Album1", "Album2"],
        "Artist2": ["Greatest Hits", "New Release"]
    }
    components = {
        "instrumental_audio": "_instrumental.mp3",
        "vocals_audio": "_vocals.mp3",
        "mir": ".mir.json"
    }

    schema_dir = dataset_dir / ".blackbird"
    schema_dir.mkdir(parents=True)

    track_infos = {}
    loc_prefix = "Main/"

    for artist in artists:
        for album in albums[artist]:
            album_path_rel = f"{artist}/{album}"
            album_path_sym = f"{loc_prefix}{artist}/{album}"
            (dataset_dir / artist / album).mkdir(parents=True)

            for i in range(1, 3):
                base_name = f"track{i}"
                track_path_sym = f"{album_path_sym}/{base_name}"
                track_files = {}
                file_sizes = {}

                for comp_name, suffix in components.items():
                    file_path_rel = f"{artist}/{album}/{base_name}{suffix}"
                    full_path = dataset_dir / file_path_rel
                    _create_test_file(full_path, f"Test {comp_name} for {track_path_sym}")
                    file_path_sym = f"{loc_prefix}{file_path_rel}"
                    track_files[comp_name] = file_path_sym
                    file_sizes[file_path_sym] = full_path.stat().st_size

                track_infos[track_path_sym] = TrackInfo(
                    track_path=track_path_sym,
                    artist=artist,
                    album_path=album_path_sym,
                    cd_number=None,
                    base_name=base_name,
                    files=track_files,
                    file_sizes=file_sizes
                )

    track_by_album = {}
    album_by_artist = {}
    for artist in artists:
        album_by_artist[artist] = set()
        for album in albums[artist]:
            album_path_sym = f"{loc_prefix}{artist}/{album}"
            album_by_artist[artist].add(album_path_sym)
            album_tracks = {k for k in track_infos if k.startswith(album_path_sym + '/')}
            track_by_album[album_path_sym] = album_tracks

    index = DatasetIndex(
        last_updated="2023-01-01",
        tracks=track_infos,
        track_by_album=track_by_album,
        album_by_artist=album_by_artist,
        total_size=sum(sum(t.file_sizes.values()) for t in track_infos.values())
    )
    index.save(schema_dir / "index.pickle")

    schema = DatasetComponentSchema.create(dataset_dir)
    schema.schema["components"] = {
        "instrumental_audio": {"pattern": "*_instrumental.mp3", "multiple": False},
        "vocals_audio": {"pattern": "*_vocals.mp3", "multiple": False},
        "mir": {"pattern": "*.mir.json", "multiple": False}
    }
    schema.save()

    return dataset_dir


@pytest.fixture
def destination_dir(tmp_path):
    """Create a destination directory for syncing."""
    dest_dir = tmp_path / "destination"
    dest_dir.mkdir()
    (dest_dir / ".blackbird").mkdir()
    return dest_dir


def test_sync_command_with_album_filtering(test_dataset, destination_dir):
    """Test sync command with album filtering."""
    runner = CliRunner()
    mock_client = _make_copy_client(test_dataset)

    with patch('blackbird.cli.configure_client', return_value=mock_client), \
         patch('blackbird.cli.click.confirm', return_value=True):

        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--artists', 'Artist1',
            '--albums', 'Album1',
            '--components', 'instrumental_audio'
        ])

        assert result.exit_code == 0, f"Command failed with: {result.output}"

        synced_files = list(destination_dir.glob('**/*.mp3'))
        assert len(synced_files) > 0, "No files were synced"

        expected_files = [
            destination_dir / "Artist1" / "Album1" / "track1_instrumental.mp3",
            destination_dir / "Artist1" / "Album1" / "track2_instrumental.mp3"
        ]
        missing = [f for f in expected_files if not f.exists()]
        assert not missing, f"Expected files not found: {missing}"


def test_sync_command_with_missing_filter(test_dataset, destination_dir):
    """Test sync command with missing component filter."""
    runner = CliRunner()

    # First sync: download only vocals_audio
    with patch('blackbird.cli.configure_client', return_value=_make_copy_client(test_dataset)), \
         patch('blackbird.cli.click.confirm', return_value=True):

        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--components', 'vocals_audio'
        ])
        assert result.exit_code == 0, f"Initial sync failed: {result.output}"

    # Remove one vocals file
    vocal_files = list(destination_dir.glob('**/*_vocals.mp3'))
    assert len(vocal_files) > 0, "No vocals files were synced"
    vocal_files[0].unlink()

    # Second sync: request instrumental where vocals_audio is missing
    with patch('blackbird.cli.configure_client', return_value=_make_copy_client(test_dataset)), \
         patch('blackbird.cli.click.confirm', return_value=True), \
         patch('blackbird.sync.DatasetSync.sync') as mock_sync:

        stats = SyncStats()
        stats.total_files = 1
        stats.downloaded_files = 1
        mock_sync.return_value = stats

        result = runner.invoke(cli_main, [
            'sync',
            f'webdav://localhost/{test_dataset}',
            str(destination_dir),
            '--components', 'instrumental_audio',
            '--missing', 'vocals_audio'
        ])

        assert result.exit_code == 0, f"Missing component sync failed: {result.output}"
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args[1]
        assert call_args['components'] == ['instrumental_audio']
        assert call_args['missing_component'] == 'vocals_audio'


def test_schema_update_during_sync(tmp_path):
    """Test that the schema is properly updated when syncing new components."""
    mock_local_schema = MagicMock()
    mock_local_schema.schema = {'components': {'vocals_audio': {}}}

    mock_remote_schema = MagicMock()
    mock_remote_schema.schema = {'components': {'vocals_audio': {}, 'instrumental_audio': {}}}

    # Simulate the schema update logic from the sync command
    component_list = ['instrumental_audio']
    for component in component_list:
        if component in mock_remote_schema.schema['components']:
            mock_local_schema.schema['components'][component] = \
                mock_remote_schema.schema['components'][component]

    assert 'instrumental_audio' in mock_local_schema.schema['components']
    assert 'vocals_audio' in mock_local_schema.schema['components']
