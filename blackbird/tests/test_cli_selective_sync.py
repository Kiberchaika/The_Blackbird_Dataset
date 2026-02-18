"""Tests for CLI selective clone workflow — schema retrieval, clone with
artist/component filters, reindexing, and stats display."""

import pytest
import json
import os
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from blackbird.cli import main as cli_main
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.schema import DatasetComponentSchema


def _make_index_and_schema(
    artists_albums_tracks: dict,
    components: dict,
) -> tuple:
    """Build a mock schema dict and DatasetIndex from a compact spec.

    Args:
        artists_albums_tracks: {artist: {album: [base_names]}}
        components: {comp_name: {"pattern": str, "multiple": bool}}

    Returns:
        (schema_dict, DatasetIndex)
    """
    schema_dict = {"version": "1.0", "components": {
        name: {**info, "description": ""} for name, info in components.items()
    }}

    index = DatasetIndex.create()
    for artist, albums in artists_albums_tracks.items():
        for album, bases in albums.items():
            album_path = f"Main/{artist}/{album}"
            for base in bases:
                track_path = f"{album_path}/{base}"
                files = {}
                file_sizes = {}
                for comp_name, comp_info in components.items():
                    pattern = comp_info["pattern"]
                    suffix = pattern.replace("*", "")
                    file_rel = f"{album_path}/{base}{suffix}"
                    files[comp_name] = file_rel
                    file_sizes[file_rel] = 1024

                track = TrackInfo(
                    track_path=track_path,
                    artist=artist,
                    album_path=album_path,
                    cd_number=None,
                    base_name=base,
                    files=files,
                    file_sizes=file_sizes,
                )
                index.tracks[track_path] = track
                index.track_by_album.setdefault(album_path, set()).add(track_path)
                index.album_by_artist.setdefault(artist, set()).add(album_path)
                index.total_size += sum(file_sizes.values())
    return schema_dict, index


# Shared dataset spec used by all tests
COMPONENTS = {
    "instrumental_audio": {"pattern": "*_instrumental.mp3", "multiple": False},
    "vocals_audio": {"pattern": "*_vocals.mp3", "multiple": False},
}

ARTISTS_ALBUMS_TRACKS = {
    "Юта": {"Хмель [2004]": ["01.Юта - Хмель", "02.Юта - Жили-были"]},
    "7Б": {"Молодые ветра [2001]": ["01.7Б - Молодые ветра"]},
    "19_84": {"Дебют [2010]": ["01.19_84 - Трек"]},
    "Other": {"OtherAlbum [2020]": ["01.Other - Song"]},
}


@pytest.fixture
def schema_and_index():
    return _make_index_and_schema(ARTISTS_ALBUMS_TRACKS, COMPONENTS)


@pytest.fixture
def mock_webdav_for_clone(schema_and_index):
    """Mock WebDAV client that simulates downloading schema, index, and audio files."""
    schema_dict, index = schema_and_index

    with patch("blackbird.sync.configure_client") as mock_configure:
        mock_client = MagicMock()
        mock_client.check_connection.return_value = True
        mock_client.base_url = "http://localhost:8080"
        mock_client.client = MagicMock()
        mock_client.client.options = {"webdav_root": "/"}

        # sync() calls client.get_index() and client.get_schema() internally
        mock_client.get_index = MagicMock(return_value=index)
        mock_schema_object = MagicMock(spec=DatasetComponentSchema)
        mock_schema_object.schema = schema_dict
        mock_client.get_schema = MagicMock(return_value=mock_schema_object)

        def download_side_effect(remote_path, local_path, file_size=None, **kwargs):
            dest = Path(local_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

            if remote_path == ".blackbird/schema.json":
                dest.write_text(json.dumps(schema_dict))
                return True
            elif remote_path == ".blackbird/index.pickle":
                index.save(dest)
                return True
            elif remote_path.endswith(".mp3"):
                dest.write_bytes(b"\x00" * 1024)
                return True
            return False

        mock_client.download_file.side_effect = download_side_effect
        mock_configure.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_webdav_for_schema(schema_and_index):
    """Mock WebDAV client that only handles schema download (for `schema show`)."""
    schema_dict, _ = schema_and_index

    with patch("blackbird.cli.configure_client") as mock_configure:
        mock_client = MagicMock()

        def download_side_effect(remote_path, local_path, file_size=None, **kwargs):
            dest = Path(local_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if remote_path == ".blackbird/schema.json":
                dest.write_text(json.dumps(schema_dict))
                return True
            return False

        mock_client.download_file.side_effect = download_side_effect
        mock_configure.return_value = mock_client
        yield mock_client


class TestSchemaShow:
    """Tests for `blackbird schema show <webdav_url>`."""

    def test_shows_remote_schema(self, mock_webdav_for_schema):
        runner = CliRunner()
        result = runner.invoke(cli_main, ["schema", "show", "webdav://localhost:8080"])

        assert result.exit_code == 0, result.output
        assert "instrumental_audio" in result.output
        assert "vocals_audio" in result.output
        assert "*_instrumental.mp3" in result.output

    def test_schema_download_called(self, mock_webdav_for_schema):
        runner = CliRunner()
        runner.invoke(cli_main, ["schema", "show", "webdav://localhost:8080"])

        # Verify the schema file was requested
        calls = mock_webdav_for_schema.download_file.call_args_list
        schema_calls = [c for c in calls if ".blackbird/schema.json" in str(c)]
        assert len(schema_calls) == 1


class TestSelectiveClone:
    """Tests for `blackbird clone` with --artists and --components filters."""

    def test_clone_with_artist_filter(self, tmp_path, mock_webdav_for_clone):
        runner = CliRunner()
        dest = tmp_path / "cloned"

        result = runner.invoke(cli_main, [
            "clone",
            "webdav://localhost:8080",
            str(dest),
            "--artists", "Юта,7Б,19_84",
            "--components", "instrumental_audio,vocals_audio",
        ])

        assert result.exit_code == 0, result.output
        assert "Clone completed" in result.output or result.exit_code == 0

    def test_clone_downloads_filtered_files(self, tmp_path, mock_webdav_for_clone):
        """Only files for selected artists should be downloaded."""
        runner = CliRunner()
        dest = tmp_path / "cloned"

        runner.invoke(cli_main, [
            "clone",
            "webdav://localhost:8080",
            str(dest),
            "--artists", "Юта,7Б",
            "--components", "instrumental_audio",
        ])

        # Check download calls — should NOT include "Other" artist
        download_calls = [
            str(c) for c in mock_webdav_for_clone.download_file.call_args_list
        ]
        other_calls = [c for c in download_calls if "Other" in c]
        assert len(other_calls) == 0, f"Files for excluded artist were downloaded: {other_calls}"

    def test_clone_with_single_component(self, tmp_path, mock_webdav_for_clone):
        """Cloning a single component should work."""
        runner = CliRunner()
        dest = tmp_path / "cloned"

        result = runner.invoke(cli_main, [
            "clone",
            "webdav://localhost:8080",
            str(dest),
            "--components", "instrumental_audio",
        ])

        assert result.exit_code == 0, result.output

    def test_clone_creates_blackbird_dir(self, tmp_path, mock_webdav_for_clone):
        """Clone should create .blackbird directory with schema and index."""
        runner = CliRunner()
        dest = tmp_path / "cloned"

        runner.invoke(cli_main, [
            "clone",
            "webdav://localhost:8080",
            str(dest),
            "--components", "instrumental_audio",
        ])

        assert (dest / ".blackbird").is_dir()
        assert (dest / ".blackbird" / "schema.json").exists()


class TestReindex:
    """Tests for `blackbird reindex <dataset_path>`."""

    def test_reindex_builds_new_index(self, tmp_path):
        """Reindex should rebuild index from files on disk."""
        dataset = tmp_path / "dataset"
        bb = dataset / ".blackbird"
        bb.mkdir(parents=True)

        schema_data = {
            "version": "1.0",
            "components": {
                "instrumental_audio": {
                    "pattern": "*_instrumental.mp3",
                    "multiple": False,
                    "description": "Instrumental"
                }
            }
        }
        (bb / "schema.json").write_text(json.dumps(schema_data))
        (bb / "locations.json").write_text(json.dumps({"Main": str(dataset)}))

        # Create some track files
        album = dataset / "Artist1" / "Album1 [2020]"
        album.mkdir(parents=True)
        (album / "01.Artist1 - Track1_instrumental.mp3").write_bytes(b"\x00" * 500)
        (album / "02.Artist1 - Track2_instrumental.mp3").write_bytes(b"\x00" * 600)

        runner = CliRunner()
        result = runner.invoke(cli_main, ["reindex", str(dataset)])

        assert result.exit_code == 0, result.output
        assert "Index rebuilt successfully" in result.output
        assert "Total tracks: 2" in result.output
        assert "instrumental_audio" in result.output
        assert (bb / "index.pickle").exists()

    def test_reindex_shows_artist_count(self, tmp_path):
        """Reindex output should show the correct artist count."""
        dataset = tmp_path / "dataset"
        bb = dataset / ".blackbird"
        bb.mkdir(parents=True)
        (bb / "schema.json").write_text(json.dumps({
            "version": "1.0",
            "components": {
                "instrumental_audio": {
                    "pattern": "*_instrumental.mp3",
                    "multiple": False,
                    "description": ""
                }
            }
        }))
        (bb / "locations.json").write_text(json.dumps({"Main": str(dataset)}))

        for artist in ["ArtistA", "ArtistB"]:
            album = dataset / artist / "Album [2020]"
            album.mkdir(parents=True)
            (album / f"01.{artist} - Track_instrumental.mp3").write_bytes(b"\x00" * 100)

        runner = CliRunner()
        result = runner.invoke(cli_main, ["reindex", str(dataset)])

        assert result.exit_code == 0, result.output
        assert "Total artists: 2" in result.output


class TestStats:
    """Tests for `blackbird stats <dataset_path>`."""

    def test_stats_local_dataset(self, tmp_path):
        """Stats should display counts and sizes for a local dataset."""
        dataset = tmp_path / "dataset"
        bb = dataset / ".blackbird"
        bb.mkdir(parents=True)
        (bb / "schema.json").write_text(json.dumps({
            "version": "1.0",
            "components": {
                "instrumental_audio": {
                    "pattern": "*_instrumental.mp3",
                    "multiple": False,
                    "description": ""
                }
            }
        }))
        (bb / "locations.json").write_text(json.dumps({"Main": str(dataset)}))

        album = dataset / "Artist1" / "Album1 [2020]"
        album.mkdir(parents=True)
        (album / "01.Artist1 - Track_instrumental.mp3").write_bytes(b"\x00" * 2048)

        # Build index first
        schema = DatasetComponentSchema(dataset)
        index = DatasetIndex.build(dataset, schema)
        index.save(bb / "index.pickle")

        runner = CliRunner()
        result = runner.invoke(cli_main, ["stats", str(dataset)])

        assert result.exit_code == 0, result.output
        assert "Total tracks: 1" in result.output
        assert "Total artists: 1" in result.output
