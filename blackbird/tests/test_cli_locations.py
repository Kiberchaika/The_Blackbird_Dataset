import pytest
import json
from pathlib import Path
from click.testing import CliRunner
from blackbird.cli import main


@pytest.fixture
def extra_dirs(tmp_path):
    """Creates two extra temporary directories for use as additional locations."""
    dir1 = tmp_path / "external_dir_1"
    dir2 = tmp_path / "external_dir_2"
    dir1.mkdir()
    dir2.mkdir()
    return dir1, dir2


@pytest.fixture
def temp_dataset_dir(tmp_path):
    """Creates a temporary dataset directory structure."""
    dataset_root = tmp_path / "dataset_cli"
    blackbird_dir = dataset_root / ".blackbird"
    blackbird_dir.mkdir(parents=True)
    schema_path = blackbird_dir / "schema.json"
    schema_path.write_text(json.dumps({"version": "1.0", "components": {}}))
    return dataset_root


@pytest.fixture
def locations_json_path(temp_dataset_dir):
    """Path to the locations.json file within the temp dataset dir."""
    return temp_dataset_dir / ".blackbird" / "locations.json"


@pytest.fixture
def runner():
    """Provides a CliRunner instance."""
    return CliRunner()


def test_location_list_default(runner, temp_dataset_dir):
    """Test `location list` with default Main location."""
    result = runner.invoke(main, ['location', 'list', str(temp_dataset_dir)])
    assert result.exit_code == 0
    assert f"Main : {temp_dataset_dir.resolve()}" in result.output


def test_location_list_multiple(runner, temp_dataset_dir, locations_json_path, extra_dirs):
    """Test `location list` with multiple locations defined in json."""
    dir1, _ = extra_dirs
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Backup": str(dir1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    result = runner.invoke(main, ['location', 'list', str(temp_dataset_dir)])
    assert result.exit_code == 0
    assert f"Main   : {temp_dataset_dir.resolve()}" in result.output
    assert f"Backup : {dir1.resolve()}" in result.output


def test_location_add_success(runner, temp_dataset_dir, locations_json_path, extra_dirs):
    """Test `location add` successfully adds a new location."""
    dir1, _ = extra_dirs
    name = "SSD_Fast"
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, str(dir1)])

    assert result.exit_code == 0
    assert f"Location '{name}' added successfully." in result.output

    assert locations_json_path.exists()
    saved_data = json.loads(locations_json_path.read_text())
    assert name in saved_data
    assert saved_data[name] == str(dir1.resolve())


def test_location_add_invalid_path(runner, temp_dataset_dir, tmp_path):
    """Test `location add` with a non-existent path."""
    non_existent = tmp_path / "non_existent_dir"
    name = "InvalidPath"
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), name, str(non_existent)])

    assert result.exit_code != 0
    assert f"Error adding location: Path '{non_existent}' does not exist" in result.output


def test_location_add_duplicate_name(runner, temp_dataset_dir, extra_dirs):
    """Test `location add` with a duplicate name ('Main')."""
    dir1, _ = extra_dirs
    result = runner.invoke(main, ['location', 'add', str(temp_dataset_dir), "Main", str(dir1)])

    assert result.exit_code != 0
    assert f"Error adding location: Location name 'Main' already exists" in result.output


def test_location_remove_success(runner, temp_dataset_dir, locations_json_path, extra_dirs):
    """Test `location remove` successfully removes a location."""
    dir1, _ = extra_dirs
    name_to_remove = "ToDelete"
    initial_loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(dir1)
    }
    locations_json_path.write_text(json.dumps(initial_loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='y\n')

    assert result.exit_code == 0
    assert f"Location '{name_to_remove}' removed successfully." in result.output

    saved_data = json.loads(locations_json_path.read_text())
    assert name_to_remove not in saved_data
    assert "Main" in saved_data


def test_location_remove_abort(runner, temp_dataset_dir, locations_json_path, extra_dirs):
    """Test `location remove` aborts when user inputs 'n'."""
    dir1, _ = extra_dirs
    name_to_remove = "KeepMe"
    initial_loc_data = {
        "Main": str(temp_dataset_dir),
        name_to_remove: str(dir1)
    }
    locations_json_path.write_text(json.dumps(initial_loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), name_to_remove], input='n\n')

    assert result.exit_code == 1
    assert "Aborted!" in result.output

    saved_data = json.loads(locations_json_path.read_text())
    assert name_to_remove in saved_data


def test_location_remove_non_existent(runner, temp_dataset_dir):
    """Test `location remove` for a location that doesn't exist."""
    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "NotFound"], input='y\n')

    assert result.exit_code != 0
    assert "Error: Location 'NotFound' does not exist" in result.output


def test_location_remove_main_disallowed(runner, temp_dataset_dir, locations_json_path, extra_dirs):
    """Test `location remove` attempting to remove 'Main' (should be allowed if others exist)."""
    dir1, _ = extra_dirs
    loc_data = {
        "Main": str(temp_dataset_dir),
        "Other": str(dir1)
    }
    locations_json_path.write_text(json.dumps(loc_data))

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "Main"], input='y\n')

    assert result.exit_code == 0
    assert "Location 'Main' removed successfully." in result.output

    saved_data = json.loads(locations_json_path.read_text())
    assert "Main" not in saved_data
    assert "Other" in saved_data


def test_location_remove_main_when_only_one(runner, temp_dataset_dir, locations_json_path):
    """Test `location remove` fails when trying to remove 'Main' as the only location."""
    if locations_json_path.exists():
        locations_json_path.unlink()

    result = runner.invoke(main, ['location', 'remove', str(temp_dataset_dir), "Main"], input='y\n')

    assert result.exit_code != 0
    assert "Error: Cannot remove the default location 'Main' when it is the only location." in result.output
