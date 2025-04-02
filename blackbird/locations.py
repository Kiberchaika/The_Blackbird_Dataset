import json
import os
from pathlib import Path
from typing import Dict, Optional

class LocationsManagerError(Exception):
    """Base exception for LocationsManager errors."""
    pass

class LocationValidationError(LocationsManagerError):
    """Error during location validation."""
    pass

class LocationsManager:
    """Manages dataset storage locations defined in .blackbird/locations.json."""

    DEFAULT_LOCATION_NAME = "Main"
    LOCATIONS_FILENAME = "locations.json"
    BLACKBIRD_DIR_NAME = ".blackbird"

    def __init__(self, dataset_root_path: Path):
        if not dataset_root_path or not dataset_root_path.is_dir():
            raise ValueError(f"Dataset root path '{dataset_root_path}' is not a valid directory.")
        self.dataset_root_path = dataset_root_path.resolve()
        self._locations: Dict[str, Path] = {}

    @property
    def locations_file_path(self) -> Path:
        """Returns the absolute path to the locations configuration file."""
        return self.dataset_root_path / self.BLACKBIRD_DIR_NAME / self.LOCATIONS_FILENAME

    def load_locations(self) -> None:
        """
        Loads location definitions from .blackbird/locations.json.

        If the file doesn't exist, initializes with a default 'Main' location
        pointing to the dataset root. If the file is invalid, raises an error.
        """
        file_path = self.locations_file_path
        loaded_locations: Dict[str, str] = {}

        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    loaded_locations = json.load(f)
                if not isinstance(loaded_locations, dict):
                    raise LocationValidationError(f"Invalid format in {file_path}. Expected a JSON object.")
                if not loaded_locations:
                    raise LocationValidationError(f"{file_path} is empty. Expected at least one location.")
            except json.JSONDecodeError as e:
                raise LocationValidationError(f"Error decoding JSON from {file_path}: {e}") from e
            except FileNotFoundError:
                # Should not happen due to exists() check, but handle defensively
                pass # Will fall through to default logic
        
        if not loaded_locations:
            # Default case: file doesn't exist or was empty/invalid and cleared
            print(f"Locations file not found or empty at {file_path}. Using default location 'Main': {self.dataset_root_path}")
            loaded_locations = {self.DEFAULT_LOCATION_NAME: str(self.dataset_root_path)}
            # We don't automatically save the default file here, only load it into memory

        # Validate and resolve paths
        validated_locations: Dict[str, Path] = {}
        for name, path_str in loaded_locations.items():
            if not isinstance(name, str) or not name:
                 raise LocationValidationError(f"Invalid location name found in {file_path}: {name!r}. Names must be non-empty strings.")
            if not isinstance(path_str, str):
                 raise LocationValidationError(f"Invalid path value for location '{name}' in {file_path}: {path_str!r}. Paths must be strings.")
                 
            path = Path(path_str)
            # Allow loading even if path doesn't exist currently, maybe it's a remote mount
            # Validation happens during add/operations
            # if not path.is_dir():
            #     raise LocationValidationError(f"Path for location '{name}' ('{path}') does not exist or is not a directory.")
            validated_locations[name] = path.resolve()
            
        self._locations = validated_locations

    def save_locations(self) -> None:
        """Saves the current location definitions to .blackbird/locations.json."""
        if not self._locations:
             raise LocationsManagerError("Cannot save empty locations. Load or add locations first.")
             
        file_path = self.locations_file_path
        blackbird_dir = file_path.parent
        
        try:
            blackbird_dir.mkdir(parents=True, exist_ok=True)
            # Convert Path objects back to strings for JSON serialization
            locations_to_save = {name: str(path) for name, path in self._locations.items()}
            with open(file_path, 'w') as f:
                json.dump(locations_to_save, f, indent=2)
        except OSError as e:
            raise LocationsManagerError(f"Error saving locations file to {file_path}: {e}") from e

    def get_location_path(self, name: str) -> Path:
        """
        Gets the absolute path for a given location name.

        Args:
            name: The name of the location.

        Returns:
            The resolved absolute Path object for the location.

        Raises:
            KeyError: If the location name is not found.
        """
        if not self._locations:
             self.load_locations() # Try loading if not already loaded
        if name not in self._locations:
             raise KeyError(f"Location '{name}' not found. Available locations: {list(self._locations.keys())}")
        return self._locations[name]

    def get_all_locations(self) -> Dict[str, Path]:
        """
        Gets a dictionary of all location names mapped to their absolute paths.

        Returns:
            A copy of the internal locations dictionary.
        """
        if not self._locations:
             self.load_locations() # Try loading if not already loaded
        return self._locations.copy()

    def add_location(self, name: str, path: Path) -> None:
        """
        Adds a new storage location. Does not save automatically.

        Args:
            name: The unique name for the location.
            path: The absolute path to the location directory.

        Raises:
            LocationValidationError: If the name or path is invalid.
        """
        if not isinstance(name, str) or not name.strip():
            raise LocationValidationError("Location name cannot be empty.")
        name = name.strip()
        if name in self._locations:
            raise LocationValidationError(f"Location name '{name}' already exists.")

        resolved_path = path.resolve()
        if not resolved_path.exists():
            raise LocationValidationError(f"Path '{resolved_path}' does not exist.")
        if not resolved_path.is_dir():
             raise LocationValidationError(f"Path '{resolved_path}' is not a directory.")

        self._locations[name] = resolved_path
        print(f"Location '{name}' added with path '{resolved_path}'. Call save_locations() to persist.")


    def remove_location(self, name: str) -> None:
        """
        Removes a storage location. Does not save automatically.

        Args:
            name: The name of the location to remove.

        Raises:
            LocationValidationError: If the name is invalid or cannot be removed.
        """
        if name not in self._locations:
            raise LocationValidationError(f"Location '{name}' does not exist.")

        if name == self.DEFAULT_LOCATION_NAME and len(self._locations) == 1:
            raise LocationValidationError(f"Cannot remove the default location '{self.DEFAULT_LOCATION_NAME}' when it is the only location.")

        del self._locations[name]
        print(f"Location '{name}' removed. Call save_locations() to persist.") 