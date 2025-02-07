from pathlib import Path
from typing import Dict, List, Optional, Set
import json
import re
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class SchemaValidationResult:
    """Result of schema validation operations."""
    is_valid: bool = True
    errors: List[str] = None
    warnings: List[str] = None
    stats: Dict = None

    def __post_init__(self):
        self.errors = self.errors or []
        self.warnings = self.warnings or []
        self.stats = self.stats or {}

    def add_error(self, msg: str):
        """Add an error message and mark result as invalid."""
        self.is_valid = False
        self.errors.append(msg)

    def add_warning(self, msg: str):
        """Add a warning message."""
        self.warnings.append(msg)


class DatasetComponentSchema:
    """Manages the schema for a dataset's components and structure."""

    SCHEMA_DIR = ".blackbird"
    SCHEMA_FILE = "schema.json"
    DEFAULT_SCHEMA = {
        "version": "1.0",
        "components": {
            "instrumental": {
                "pattern": "*_instrumental.mp3",
                "required": True
            }
        },
        "structure": {
            "artist_album_format": {
                "levels": ["artist", "album", "?cd", "track"],
                "cd_pattern": "CD\\d+",
                "is_cd_optional": True
            }
        },
        "sync": {
            "default_components": ["instrumental"],
            "exclude_patterns": ["*.tmp", "*.bak"]
        }
    }

    def __init__(self, dataset_path: Path):
        """Initialize schema for a dataset.
        
        Args:
            dataset_path: Path to the dataset root directory
        """
        self.dataset_path = Path(dataset_path)
        self.schema_path = self.dataset_path / self.SCHEMA_DIR / self.SCHEMA_FILE
        self._load()

    @classmethod
    def create(cls, dataset_path: Path) -> 'DatasetComponentSchema':
        """Create a new schema with default structure.
        
        Args:
            dataset_path: Path to create the schema in
            
        Returns:
            New schema instance
        """
        schema_dir = dataset_path / cls.SCHEMA_DIR
        schema_dir.mkdir(parents=True, exist_ok=True)
        
        schema_path = schema_dir / cls.SCHEMA_FILE
        with open(schema_path, 'w') as f:
            json.dump(cls.DEFAULT_SCHEMA, f, indent=2)
            
        return cls(dataset_path)

    def _load(self):
        """Load schema from file or create with defaults if it doesn't exist."""
        try:
            with open(self.schema_path) as f:
                self.schema = json.load(f)
        except FileNotFoundError:
            self.schema = self.DEFAULT_SCHEMA.copy()
            self.save()

    def save(self):
        """Save current schema to file."""
        self.schema_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.schema_path, 'w') as f:
            json.dump(self.schema, f, indent=2)

    def add_component(
        self,
        name: str,
        pattern: str,
        required: bool = False,
        multiple: bool = False,
        description: Optional[str] = None,
        dry_run: bool = False
    ) -> SchemaValidationResult:
        """Add a new component to schema with validation.
        
        Args:
            name: Component identifier
            pattern: Glob pattern for files
            required: Whether component is required for all tracks
            multiple: Whether multiple files per track are allowed
            description: Optional description
            dry_run: If True, only validate but don't save
            
        Returns:
            Validation result with statistics
        """
        result = SchemaValidationResult()
        
        # Validate component name
        if not name.isidentifier():
            result.add_error(f"Invalid component name: {name}")
        
        # Check if component already exists
        if name in self.schema["components"]:
            result.add_error(f"Component {name} already exists")
            
        # Test pattern against dataset
        found_files = list(self.dataset_path.rglob(pattern))
        if not found_files:
            result.add_warning(f"Pattern {pattern} matches no files")
        
        result.stats["matched_files"] = len(found_files)
        
        # Group by track to check multiple/single file constraints
        tracks_files = self._group_by_track(found_files)
        if not multiple:
            multiple_files = [track for track, files in tracks_files.items() if len(files) > 1]
            if multiple_files:
                result.add_error(
                    f"Multiple files found for {len(multiple_files)} tracks "
                    f"but multiple=False (example: {multiple_files[0]})"
                )
        
        result.stats["matched_tracks"] = len(tracks_files)
        
        # If validation passed and not dry run, add to schema
        if result.is_valid and not dry_run:
            self.schema["components"][name] = {
                "pattern": pattern,
                "required": required,
                "multiple": multiple
            }
            if description:
                self.schema["components"][name]["description"] = description
            self.save()
            
        return result

    def remove_component(
        self,
        name: str,
        dry_run: bool = False
    ) -> SchemaValidationResult:
        """Remove a component from schema with validation.
        
        Args:
            name: Component to remove
            dry_run: If True, only validate but don't save
            
        Returns:
            Validation result
        """
        result = SchemaValidationResult()
        
        # Check if component exists
        if name not in self.schema["components"]:
            result.add_error(f"Component {name} does not exist")
            return result
            
        # Check if component is required
        if self.schema["components"][name].get("required", False):
            result.add_error(f"Cannot remove required component {name}")
            return result
            
        # If not dry run, remove component
        if not dry_run:
            del self.schema["components"][name]
            self.save()
            
        return result

    def validate(self) -> SchemaValidationResult:
        """Validate entire schema against dataset.
        
        Returns:
            Validation result with statistics
        """
        result = SchemaValidationResult()
        
        # Check each component
        for name, config in self.schema["components"].items():
            pattern = config["pattern"]
            required = config.get("required", False)
            multiple = config.get("multiple", False)
            
            # Find matching files
            found_files = list(self.dataset_path.rglob(pattern))
            tracks_files = self._group_by_track(found_files)
            
            result.stats[name] = {
                "matched_files": len(found_files),
                "matched_tracks": len(tracks_files)
            }
            
            # Validate requirements
            if required and not found_files:
                result.add_error(f"Required component {name} has no matching files")
                
            # Validate multiple constraint
            if not multiple:
                multiple_files = [track for track, files in tracks_files.items() if len(files) > 1]
                if multiple_files:
                    result.add_error(
                        f"Component {name} has multiple files for {len(multiple_files)} tracks "
                        f"but multiple=False"
                    )
        
        # Validate directory structure
        if not self._validate_directory_structure(result):
            result.add_error("Invalid directory structure found")
        
        return result

    def _validate_directory_structure(self, result: SchemaValidationResult) -> bool:
        """Validate the directory structure matches the schema.
        
        Args:
            result: Validation result to update with findings
            
        Returns:
            True if structure is valid, False otherwise
        """
        structure = self.schema["structure"]["artist_album_format"]
        cd_pattern = re.compile(structure["cd_pattern"])
        is_valid = True
        
        # Track statistics about directory structure
        level_counts = defaultdict(int)
        
        for path in self.dataset_path.rglob("*_instrumental.mp3"):
            relative_path = path.relative_to(self.dataset_path)
            parts = list(relative_path.parts)
            
            # Should have at least artist/album/track
            if len(parts) < 3:
                result.add_error(f"Invalid path depth for {relative_path}")
                is_valid = False
                continue
                
            # Check if CD directory is present
            if len(parts) == 4:
                cd_dir = parts[2]
                if not cd_pattern.match(cd_dir):
                    result.add_error(f"Invalid CD directory format: {cd_dir}")
                    is_valid = False
            elif len(parts) > 4:
                result.add_error(f"Path too deep: {relative_path}")
                is_valid = False
            
            level_counts[len(parts)] += 1
            
        # Add statistics
        result.stats["directory_structure"] = {
            "level_counts": dict(level_counts)
        }
        
        return is_valid

    def get_track_relative_path(self, path: Path) -> str:
        """Get the relative path that uniquely identifies a track within the dataset.
        
        This path starts from the dataset root and includes artist/album/[cd]/track structure.
        For example:
        - Regular track: "Artist/Album/01.Track"
        - CD track: "Artist/Album/CD1/01.Track"
        
        Args:
            path: Path to any file belonging to the track
            
        Returns:
            Relative path from dataset root that uniquely identifies the track
        """
        # Get relative path from dataset root
        rel_path = path.relative_to(self.dataset_path)
        parts = list(rel_path.parts)
        
        # Include CD directory in path if present
        if len(parts) == 4:  # artist/album/cd/track
            cd_dir = parts[2]
            if re.match(self.schema["structure"]["artist_album_format"]["cd_pattern"], cd_dir):
                return str(Path(*parts[:-1]) / self._get_base_name(parts[-1]))
        
        # For non-CD tracks, use artist/album/basename
        return str(Path(*parts[:-1]) / self._get_base_name(parts[-1]))

    def _get_base_name(self, filename: str) -> str:
        """Get base name without component suffixes.
        
        This is used to match companion files within the same directory.
        For example, "01.Artist - Track" from "01.Artist - Track_instrumental.mp3"
        
        Args:
            filename: Name of the file (without path)
            
        Returns:
            Base name without component suffixes or extensions
        """
        name = filename
        # Remove component suffixes
        for suffix in ['_instrumental', '_vocals_noreverb', '_vocals_stretched_120bpm_section']:
            name = name.replace(suffix, '')
        # Remove extensions
        if name.endswith('.mp3'):
            name = name[:-4]
        elif name.endswith('.mir.json'):
            name = name[:-9]  # Remove both .mir.json
        elif name.endswith('.json'):
            name = name[:-5]
        return name

    def _group_by_track(self, files: List[Path]) -> Dict[str, List[Path]]:
        """Group files by their track path.
        
        Args:
            files: List of file paths
            
        Returns:
            Dictionary mapping track paths to lists of files
        """
        tracks = defaultdict(list)
        for f in files:
            track_path = self.get_track_relative_path(f)
            tracks[track_path].append(f)
        return tracks

    def find_companion_files(self, track_path: Path) -> List[Path]:
        """Find all companion files for a track in its directory.
        
        Args:
            track_path: Path to any file belonging to the track
            
        Returns:
            List of all files belonging to the same track
        """
        base_name = self._get_base_name(track_path.name)
        companions = []
        for f in track_path.parent.iterdir():
            if f.is_file():
                if self._get_base_name(f.name) == base_name:
                    companions.append(f)
        return sorted(companions)
