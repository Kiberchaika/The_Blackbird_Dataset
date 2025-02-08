from pathlib import Path
from typing import Dict, List, Optional, Set
import json
import re
from dataclasses import dataclass
from collections import defaultdict
import fnmatch


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
            "instrumental_audio": {
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
        dry_run: bool = False,
        skip_file_validation: bool = False
    ) -> SchemaValidationResult:
        """Add a new component to schema with validation.
        
        Args:
            name: Component identifier
            pattern: Glob pattern for files
            required: Whether component is required for all tracks
            multiple: Whether multiple files per track are allowed
            description: Optional description
            dry_run: If True, only validate but don't save
            skip_file_validation: If True, skip validating file existence
            
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
            
        if not skip_file_validation:
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
        else:
            # Skip file validation
            result.stats["matched_files"] = 0
            result.stats["matched_tracks"] = 0
            result.stats["validation_skipped"] = True
        
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

    def validate(self, skip_file_validation: bool = False) -> SchemaValidationResult:
        """Validate entire schema against dataset.
        
        Args:
            skip_file_validation: If True, skip validating file existence
                                (useful during initial sync)
        
        Returns:
            Validation result with statistics
        """
        result = SchemaValidationResult()
        
        # Check each component
        for name, config in self.schema["components"].items():
            pattern = config["pattern"]
            required = config.get("required", False)
            multiple = config.get("multiple", False)
            
            if not skip_file_validation:
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
            else:
                # Skip file validation, just validate component configuration
                result.stats[name] = {
                    "matched_files": 0,
                    "matched_tracks": 0,
                    "validation_skipped": True
                }
        
        # Validate directory structure
        if not skip_file_validation and not self._validate_directory_structure(result):
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
        # Remove extension (everything after the last dot)
        if '.' in name:
            name = name.rsplit('.', 1)[0]
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

    def discover_schema(self, folders: Optional[List[str]] = None) -> SchemaValidationResult:
        """Analyze dataset to discover and generate a schema that matches its structure.
        
        This method:
        1. Scans the dataset directory structure
        2. Identifies unique file patterns
        3. Determines required/optional components
        4. Generates appropriate glob patterns
        
        Args:
            folders: Optional list of folders to analyze (e.g. ["Artist1/Album1"]).
                    If None, analyzes entire dataset.
        
        Returns:
            SchemaValidationResult with discovered schema and statistics
        """
        result = SchemaValidationResult()
        discovered_components = {}
        
        # Find all files in dataset
        if folders:
            # Only scan specified folders
            all_files = []
            for folder in folders:
                folder_path = self.dataset_path / folder
                if not folder_path.exists():
                    result.add_warning(f"Folder not found: {folder}")
                    continue
                all_files.extend(folder_path.rglob("*"))
        else:
            # Scan entire dataset
            all_files = list(self.dataset_path.rglob("*"))

        # First find all instrumental files to determine total tracks
        instrumental_files = [f for f in all_files if f.is_file() and f.name.endswith('_instrumental.mp3')]
        total_tracks = len(instrumental_files)  # Each track should have exactly one instrumental file
        if total_tracks == 0:
            result.add_error("No tracks found in the specified folders")
            return result

        # Track all unique track paths
        all_track_paths = {self._get_track_path(f) for f in instrumental_files}
        
        print("\nFound instrumental files:")
        for f in instrumental_files:
            print(f"  {f.name} -> {self._get_track_path(f)}")
        print(f"\nTotal tracks: {total_tracks}")
        print(f"Unique track paths: {len(all_track_paths)}")

        # First pass: Group files by their base pattern to identify components
        files_by_base_pattern = defaultdict(list)
        
        for file in all_files:
            if file.is_file():
                # Skip hidden files and directories
                if any(part.startswith('.') for part in file.parts):
                    continue
                
                # Get track path for this file
                track_path = self._get_track_path(file)
                
                # Get the base name and analyze the suffix
                base_name = file.stem.split('_')[0]
                remaining = file.name[len(base_name):]
                
                # Skip if no pattern to analyze
                if not remaining:
                    continue
                
                # Split into meaningful parts
                parts = remaining.split('_')
                if len(parts) > 1:  # Has component identifiers
                    # Extract the main component identifier
                    component_parts = []
                    for part in parts[1:]:
                        if 'section' in part:
                            break  # Stop at section marker
                        if not any(c.isdigit() for c in part):
                            component_parts.append(part)
                    
                    if component_parts:
                        component_id = '_'.join(component_parts)
                        # Don't include extension in component name
                        component_id = component_id.split('.')[0]
                        
                        # Get the extension (without the dot)
                        extension = file.suffix.lstrip('.')
                        
                        # Create component ID with extension
                        if extension == 'json':
                            component_id = f"{component_id}_lyrics"  # Special case for JSON files containing lyrics
                        elif extension == 'mp3':
                            component_id = f"{component_id}_audio"  # Audio files
                        
                        # Special case for instrumental files
                        if 'instrumental' in component_id.lower():
                            if extension == 'mp3':
                                files_by_base_pattern['instrumental_audio'].append(file)
                            elif extension == 'json':
                                files_by_base_pattern['instrumental_lyrics'].append(file)
                        else:
                            files_by_base_pattern[component_id].append(file)
                        
                        # If this is a section file, also add to sections group
                        if 'section' in remaining:
                            section_id = f"{component_id}_section"
                            files_by_base_pattern[section_id].append(file)
                
                elif file.name.endswith('.mir.json'):  # Special case for MIR files
                    files_by_base_pattern['mir'].append(file)
                
                # Also check for instrumental files in the full name
                if '_instrumental.' in file.name.lower():
                    if file.suffix.lower() == '.mp3':
                        files_by_base_pattern['instrumental_audio'].append(file)
                    elif file.suffix.lower() == '.json':
                        files_by_base_pattern['instrumental_lyrics'].append(file)
        
        # Calculate total tracks from unique track paths
        total_tracks = len(all_track_paths)
        if total_tracks == 0:
            result.add_error("No tracks found in the specified folders")
            return result

        # Second pass: Analyze each component group to determine patterns and properties
        for base_pattern, files in files_by_base_pattern.items():
            # Skip if very few files with this pattern
            if len(files) < total_tracks * 0.05:  # Less than 5% coverage
                continue
            
            # Analyze file patterns in this group
            extensions = {f.suffix for f in files}
            has_sections = 'section' in base_pattern
            
            # Generate component name
            component_name = base_pattern.lower()
            if not component_name.isidentifier():
                component_name = ''.join(c for c in component_name if c.isalnum() or c == '_')
                if not component_name.isidentifier():
                    continue
            
            # Generate pattern based on file analysis
            if has_sections:
                # Files with numbered sections
                pattern = f"*_{base_pattern.replace('_section', '')}_*section*{next(iter(extensions))}"  # Use the first extension
            elif component_name == 'mir':
                pattern = "*.mir.json"
            elif '_audio' in component_name:
                pattern = f"*_{base_pattern.replace('_audio', '')}.mp3"
            elif '_lyrics' in component_name:
                pattern = f"*_{base_pattern.replace('_lyrics', '')}.json"
            else:
                # Regular component files - use specific extension
                pattern = f"*_{base_pattern}{next(iter(extensions))}"  # Use the first extension
            
            # Group files by track for coverage calculation
            tracks_with_component = defaultdict(list)
            for file in files:
                track_path = self._get_track_path(file)
                tracks_with_component[track_path].append(file)
            
            # Calculate coverage based on unique tracks
            track_coverage = len(tracks_with_component) / total_tracks if total_tracks > 0 else 0
            
            # Determine if multiple files per track exist
            has_multiple = max(len(files) for files in tracks_with_component.values()) > 1
            
            # Add component to discovered schema
            discovered_components[component_name] = {
                "pattern": pattern,
                "multiple": has_multiple or has_sections or component_name in ['vocals_noreverb', 'instrumental'],  # Some components always allow multiple
                "description": f"Auto-discovered {component_name} files"
            }
            
            # Add statistics
            result.stats[component_name] = {
                "file_count": len(files),
                "track_coverage": track_coverage,
                "has_multiple": has_multiple,
                "unique_tracks": len(tracks_with_component),
                "max_files_per_track": max(len(files) for files in tracks_with_component.values()),
                "min_files_per_track": min(len(files) for files in tracks_with_component.values()),
                "extensions": list(extensions),
                "has_sections": has_sections
            }
        
        # Add any missing required components
        for component_name in ['instrumental_audio']:
            if component_name not in discovered_components and component_name in files_by_base_pattern:
                files = files_by_base_pattern[component_name]
                if len(files) >= total_tracks * 0.05:  # At least 5% coverage
                    # Add component to schema
                    discovered_components[component_name] = {
                        "pattern": "*_instrumental.*",
                        "multiple": True,  # Always allow multiple
                        "description": f"Auto-discovered {component_name} files"
                    }
                    
                    # Add statistics
                    tracks_with_component = defaultdict(list)
                    for file in files:
                        track_path = self._get_track_path(file)
                        tracks_with_component[track_path].append(file)
                    
                    result.stats[component_name] = {
                        "file_count": len(files),
                        "track_coverage": len(tracks_with_component) / total_tracks,
                        "has_multiple": True,
                        "unique_tracks": len(tracks_with_component),
                        "max_files_per_track": max(len(files) for files in tracks_with_component.values()),
                        "min_files_per_track": min(len(files) for files in tracks_with_component.values()),
                        "extensions": list({f.suffix for f in files}),
                        "has_sections": False
                    }
        
        # Print discovered schema
        print("\nDiscovered Schema:")
        print(json.dumps(discovered_components, indent=2))
        
        # Update schema with discovered components
        self.schema["components"] = discovered_components
        self.save()
        
        return result

    def validate_against_data(self, dataset_path: Optional[Path] = None) -> SchemaValidationResult:
        """Validate that schema correctly describes an existing dataset's structure.
        
        Args:
            dataset_path: Optional alternative dataset path to validate against
                        (defaults to self.dataset_path)
        
        Returns:
            SchemaValidationResult with validation details
        """
        result = SchemaValidationResult()
        path = dataset_path or self.dataset_path
        
        # Track statistics
        total_files = 0
        matched_files = 0
        component_stats = defaultdict(lambda: {"matched": 0, "unmatched": 0})
        
        # Find all music files
        all_files = list(path.rglob("*"))
        music_files = [f for f in all_files if f.is_file() and not any(part.startswith('.') for part in f.parts)]
        total_files = len(music_files)
        
        # Track which files are matched by components
        matched_by_component = defaultdict(set)
        unmatched_files = set(music_files)
        
        # Check each component's patterns
        for component_name, config in self.schema["components"].items():
            pattern = config["pattern"]
            required = config.get("required", False)
            multiple = config.get("multiple", False)
            
            # Find all files matching this component
            matching_files = set()
            tracks_with_component = defaultdict(list)
            
            for file in music_files:
                if fnmatch.fnmatch(file.name, pattern):
                    matching_files.add(file)
                    if file in unmatched_files:
                        unmatched_files.remove(file)
                    matched_by_component[component_name].add(file)
                    
                    # Group by track for validation
                    track_path = self._get_track_path(file)
                    tracks_with_component[track_path].append(file)
            
            # Update component statistics
            component_stats[component_name]["matched"] = len(matching_files)
            matched_files += len(matching_files)
            
            # Validate required components
            if required and matching_files:  # Only check if we found any files
                missing_tracks = []
                for track in self._get_all_track_paths(path):
                    if track not in tracks_with_component:
                        missing_tracks.append(track)
                
                if missing_tracks:
                    result.add_error(
                        f"Required component '{component_name}' missing for tracks: "
                        f"{', '.join(str(t) for t in missing_tracks[:5])} "
                        f"{'and more' if len(missing_tracks) > 5 else ''}"
                    )
            
            # Validate multiple files constraint
            if not multiple:
                tracks_with_multiple = []
                for track_path, files in tracks_with_component.items():
                    if len(files) > 1:
                        tracks_with_multiple.append(track_path)
                
                if tracks_with_multiple:
                    result.add_error(
                        f"Component '{component_name}' has multiple files for tracks: "
                        f"{', '.join(str(t) for t in tracks_with_multiple[:5])} "
                        f"{'and more' if len(tracks_with_multiple) > 5 else ''}"
                    )
        
        # Check for pattern collisions
        for comp1 in self.schema["components"]:
            for comp2 in self.schema["components"]:
                if comp1 >= comp2:
                    continue
                collision = matched_by_component[comp1] & matched_by_component[comp2]
                if collision:
                    result.add_error(
                        f"Pattern collision between '{comp1}' and '{comp2}' for files: "
                        f"{', '.join(str(f) for f in list(collision)[:5])} "
                        f"{'and more' if len(collision) > 5 else ''}"
                    )
        
        # Add statistics to result
        result.stats.update({
            "total_files": total_files,
            "matched_files": matched_files,
            "unmatched_files": len(unmatched_files),
            "component_coverage": {
                name: stats for name, stats in component_stats.items()
            }
        })
        
        # Add warning for unmatched files
        if unmatched_files:
            result.add_warning(
                f"Found {len(unmatched_files)} files not matching any component pattern: "
                f"{', '.join(str(f) for f in list(unmatched_files)[:5])} "
                f"{'and more' if len(unmatched_files) > 5 else ''}"
            )
        
        return result

    def _get_track_path(self, file_path: Path) -> str:
        """Get the canonical track path for a file.
        
        Args:
            file_path: Path to a file
            
        Returns:
            String representation of track path (artist/album/[cd]/track_base)
        """
        relative_path = file_path.relative_to(self.dataset_path)
        parts = list(relative_path.parts)
        
        # Get base name without component suffix and number prefix
        base_name = file_path.stem
        if '_' in base_name:
            base_name = base_name.split('_')[0]
        if '.' in base_name:
            base_name = base_name.split('.', 1)[1]  # Remove the track number prefix
        
        # Handle different directory depths
        if len(parts) >= 3 and re.match(r"CD\d+", parts[2]):
            # Has CD directory
            return f"{parts[0]}/{parts[1]}/{parts[2]}/{base_name}"
        elif len(parts) >= 2:
            # Regular artist/album structure
            return f"{parts[0]}/{parts[1]}/{base_name}"
        elif len(parts) == 1:
            # Single album folder
            return base_name
        else:
            # Shouldn't happen, but handle gracefully
            return base_name

    def _get_all_track_paths(self, path: Path) -> Set[str]:
        """Get all unique track paths in the dataset.
        
        Args:
            path: Dataset root path
            
        Returns:
            Set of track paths
        """
        track_paths = set()
        for file in path.rglob("*_instrumental.mp3"):
            track_paths.add(self._get_track_path(file))
        return track_paths
