from pathlib import Path
import json
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import fnmatch
import pickle
import os
from collections import defaultdict

@dataclass
class ValidationResult:
    """Result of schema validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    stats: Dict[str, Any]

    def add_error(self, error: str) -> None:
        """Add an error message and set is_valid to False.
        
        Args:
            error: Error message to add
        """
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning message.

        Args:
            warning: Warning message to add
        """
        self.warnings.append(warning)

class DatasetComponentSchema:
    """Schema for dataset components and structure."""

    def __init__(self, dataset_path: Path):
        """Initialize schema manager.
        
        Args:
            dataset_path: Path to dataset root
        """
        self.dataset_path = Path(dataset_path)
        self.schema_path = self.dataset_path / ".blackbird" / "schema.json"
        self.schema = self._load()

    @classmethod
    def create(cls, dataset_path: Path) -> 'DatasetComponentSchema':
        """Create a new schema for the dataset.
        
        Args:
            dataset_path: Path to dataset root
            
        Returns:
            New schema instance
        """
        schema = cls(dataset_path)
        schema.schema_path.parent.mkdir(parents=True, exist_ok=True)
        schema.save()
        return schema

    def save(self) -> None:
        """Save schema to file."""
        self.schema_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.schema_path, 'w', encoding='utf-8') as f:
            json.dump(self.schema, f, indent=2, ensure_ascii=False)

    def validate(self) -> ValidationResult:
        """Validate schema against dataset structure.
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], stats={
            "directory_structure": {
                "artists": 0,
                "albums": 0,
                "cds": 0,
                "tracks": 0
            }
        })
        
        # Check for pattern collisions first
        seen_patterns = set()
        for comp_name, comp_info in self.schema["components"].items():
            pattern = comp_info["pattern"]
            if pattern in seen_patterns:
                result.add_error(f"Pattern collision between components: {pattern}")
                return result  # Return early if we find a collision
            seen_patterns.add(pattern)
        
        # Check directory structure
        for root, _, files in os.walk(self.dataset_path):
            try:
                rel_path = Path(root).relative_to(self.dataset_path)
            except ValueError:
                continue  # Skip root directory
                
            parts = rel_path.parts
            
            # Skip .blackbird directory
            if '.blackbird' in parts:
                continue
                
            # Validate directory structure
            if len(parts) > 0:
                # Artist level
                if len(parts) == 1:
                    result.stats["directory_structure"]["artists"] += 1
                # Album level
                elif len(parts) == 2:
                    result.stats["directory_structure"]["albums"] += 1
                # CD level (optional)
                elif len(parts) == 3:
                    if not parts[2].startswith('CD') or not parts[2][2:].isdigit():
                        result.add_error(f"Invalid CD directory format: {parts[2]} (must be CD followed by digits)")
                    result.stats["directory_structure"]["cds"] += 1
                # Track level
                elif len(parts) > 3:
                    result.add_error(f"Path too deep: {rel_path}")
                    
                # Count tracks by looking at instrumental files
                for file in files:
                    if "_instrumental.mp3" in file:
                        result.stats["directory_structure"]["tracks"] += 1
        
        return result

    def discover_schema(self, folders: Optional[List[str]] = None) -> ValidationResult:
        """Discover schema from dataset files.
        
        Args:
            folders: Optional list of folders to analyze (relative to dataset root)
            
        Returns:
            Validation result with discovered schema
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], stats={})
        
        # Try to load index if available
        index_path = self.dataset_path / ".blackbird" / "index.pickle"
        if index_path.exists():
            with open(index_path, 'rb') as f:
                index = pickle.load(f)
                return self._discover_from_index(index, folders, result)
        
        # If no index, discover directly from filesystem
        return self._discover_from_filesystem(folders, result)

    def _discover_from_index(self, index, folders, result):
        # Reset components for discovery
        self.schema["components"] = {}
            
        # Collect all unique file patterns
        patterns = defaultdict(lambda: {
            "count": 0,
            "tracks": set(),
            "files_per_track": defaultdict(int),
            "extensions": set(),
            "has_sections": False
        })
        
        # Process each track
        for track_path, track_info in index.tracks.items():
            # Skip if not in requested folders
            if folders:
                if not any(track_path.startswith(f) for f in folders):
                    continue
                    
            # Group files by pattern
            for file_path in track_info.files:
                file_name = Path(file_path).name
                base_name = Path(file_path).stem
                
                # Extract pattern
                pattern = None
                component_name = None
                
                if "_instrumental.mp3" in file_name:
                    pattern = "*_instrumental.mp3"
                    component_name = "instrumental"
                elif "_vocals_noreverb.mp3" in file_name:
                    pattern = "*_vocals_noreverb.mp3"
                    component_name = "vocals_noreverb"
                elif ".mir.json" in file_name:
                    pattern = "*.mir.json"
                    component_name = "mir"
                elif "_section" in file_name:
                    pattern = f"*{file_name[len(base_name):]}"
                    component_name = "section"
                    patterns[pattern]["has_sections"] = True
                
                if pattern and component_name:
                    patterns[pattern]["count"] += 1
                    patterns[pattern]["tracks"].add(track_path)
                    patterns[pattern]["files_per_track"][track_path] += 1
                    patterns[pattern]["extensions"].add(Path(file_name).suffix)
                    patterns[pattern]["component_name"] = component_name
        
        # Update schema components
        for pattern, stats in patterns.items():
            component_name = stats.get("component_name", pattern[1:].split('.')[0])
            
            # Add component
            self.schema["components"][component_name] = {
                "pattern": pattern,
                "required": component_name == "instrumental",
                "multiple": stats["has_sections"] or max(stats["files_per_track"].values()) > 1
            }
            
            # Add statistics
            result.stats[component_name] = {
                "file_count": stats["count"],
                "track_coverage": len(stats["tracks"]) / len(index.tracks),
                "unique_tracks": len(stats["tracks"]),
                "min_files_per_track": min(stats["files_per_track"].values()),
                "max_files_per_track": max(stats["files_per_track"].values()),
                "extensions": sorted(stats["extensions"]),
                "has_sections": stats["has_sections"]
            }
        
        return result

    def _get_pattern_from_path(self, file_path: str) -> str:
        """Get pattern from file path."""
        base_name = os.path.basename(file_path)
        return f"*{base_name[base_name.find('_'):]}"

    def _discover_from_filesystem(self, folders: Optional[List[str]], result: ValidationResult) -> ValidationResult:
        """Discover schema by scanning filesystem.
        
        Args:
            folders: Optional list of folders to analyze
            result: ValidationResult to update
            
        Returns:
            Updated validation result
        """
        # Reset components for discovery
        self.schema["components"] = {}
        
        # Initialize result stats
        result.stats = {
            'unmatched': {
                'file_count': 0,
                'track_coverage': 0,
                'unique_tracks': 0,
                'min_files_per_track': 0,
                'max_files_per_track': 0,
                'extensions': [],
                'has_sections': False,
                'has_multiple': False
            }
        }

        # First pass: collect all files and their patterns
        pattern_stats = {}
        pattern_groups = defaultdict(list)
        for track_path in self._list_tracks(self.dataset_path):
            # Skip if not in requested folders
            if folders and not any(track_path.startswith(f) for f in folders):
                continue
                
            track_files = self._list_track_files(track_path)
            for file_path in track_files:
                pattern = self._get_pattern_from_path(file_path)
                if pattern not in pattern_stats:
                    pattern_stats[pattern] = {
                        'count': 0,
                        'tracks': set(),
                        'files_per_track': defaultdict(int),
                        'extensions': set(),
                        'has_sections': False,
                        'has_multiple': False,
                        'component_name': None,
                        'examples': set(),
                        'base_component': None
                    }
                stats = pattern_stats[pattern]
                stats['count'] += 1
                stats['tracks'].add(track_path)
                stats['files_per_track'][track_path] += 1
                stats['extensions'].add(os.path.splitext(file_path)[1])
                stats['examples'].add(os.path.basename(file_path))

                # Extract component name from pattern
                base_name = Path(file_path).stem
                parts = base_name.split('_')
                
                # Check for numbered components (e.g. vocals1, vocals2, etc.)
                component_name = None
                for part in parts[1:]:  # Skip track name
                    if part.rstrip('123456789') and part.endswith(tuple('123456789')):
                        component_name = part.rstrip('123456789')
                        stats['has_multiple'] = True
                        stats['base_component'] = component_name
                        break
                
                if not component_name:
                    component_name = '_'.join(parts[1:])  # Everything after track name
                
                stats['component_name'] = component_name
                pattern_groups[component_name].append(pattern)
                
                print(f"\nPattern: {pattern}")
                print(f"Stats: {stats}")

        print(f"\nComponent groups: {pattern_groups.keys()}")
        
        # Second pass: Process each component group
        processed_components = set()
        for component_name, patterns in pattern_groups.items():
            print(f"\nProcessing pattern group for component: {component_name}")
            
            if component_name in processed_components:
                continue
            
            # Check if this is a numbered component group
            base_component = None
            for pattern in patterns:
                stats = pattern_stats[pattern]
                if stats.get('base_component'):
                    base_component = stats['base_component']
                    break
            
            if base_component:
                # Find all patterns that belong to this base component
                component_patterns = []
                unmatched_patterns = []
                for c_name, c_patterns in pattern_groups.items():
                    if c_name == base_component or any(pattern_stats[p].get('base_component') == base_component for p in c_patterns):
                        component_patterns.extend(c_patterns)
                        processed_components.add(c_name)
                    elif c_name.startswith(base_component):
                        # These are unmatched patterns that start with the base component name
                        unmatched_patterns.extend(c_patterns)
                
                # Aggregate stats for the base component
                all_tracks = set()
                all_extensions = set()
                files_per_track = defaultdict(int)
                unmatched_count = 0
                
                for pattern in component_patterns:
                    stats = pattern_stats[pattern]
                    all_tracks.update(stats['tracks'])
                    all_extensions.update(stats['extensions'])
                
                # Calculate total count based on tracks and files per track
                total_count = len(all_tracks) * 3  # Each track has exactly 3 numbered files
                
                # Count unmatched files
                for pattern in unmatched_patterns:
                    unmatched_count += pattern_stats[pattern]['count']
                
                # Update result stats for the base component
                result.stats[base_component] = {
                    'file_count': total_count,
                    'track_coverage': len(all_tracks),
                    'unique_tracks': len(all_tracks),
                    'min_files_per_track': 3,  # Each track has exactly 3 files
                    'max_files_per_track': 3,  # Each track has exactly 3 files
                    'extensions': list(all_extensions),
                    'has_sections': False,
                    'has_multiple': True,
                    'unmatched': unmatched_count  # Add unmatched files count
                }
                
                # Add component to schema
                self.schema['components'][base_component] = {
                    'pattern': f'*_{base_component}[0-9]+{next(iter(all_extensions))}',
                    'multiple': True,
                    'required': False
                }
            else:
                # Handle non-numbered components
                pattern = patterns[0]  # Use first pattern as representative
                stats = pattern_stats[pattern]
                
                # Update result stats for this component
                result.stats[component_name] = {
                    'file_count': stats['count'],
                    'track_coverage': len(stats['tracks']),
                    'unique_tracks': len(stats['tracks']),
                    'min_files_per_track': min(stats['files_per_track'].values()),
                    'max_files_per_track': max(stats['files_per_track'].values()),
                    'extensions': list(stats['extensions']),
                    'has_sections': stats['has_sections'],
                    'has_multiple': stats['has_multiple'],
                    'unmatched': 0  # Non-numbered components don't have unmatched files
                }
                
                # Add component to schema
                self.schema['components'][component_name] = {
                    'pattern': pattern,
                    'multiple': stats['has_multiple'],
                    'required': component_name == 'instrumental'  # Only instrumental is required
                }
            processed_components.add(component_name)

        # Add structure and sync sections if not present
        if "structure" not in self.schema:
            self.schema["structure"] = {
                "artist_album_format": {
                    "levels": ["artist", "album", "?cd", "track"],
                    "cd_pattern": "CD\\d+",
                    "is_cd_optional": True
                }
            }
            
        if "sync" not in self.schema:
            self.schema["sync"] = {
                "exclude_patterns": ["*.tmp", "*.bak"]
            }
        
        return result

    def find_companion_files(self, file_path: Path) -> List[Path]:
        """Find companion files for a track.
        
        Args:
            file_path: Path to track file
            
        Returns:
            List of companion file paths
        """
        companions = []
        base_path = self.get_track_relative_path(file_path)
        
        # Check each component pattern
        for comp_info in self.schema["components"].values():
            pattern = comp_info["pattern"]
            if pattern.startswith('*'):
                # Convert glob pattern to potential companion path
                companion = self.dataset_path / (base_path + pattern[1:])
                if companion.exists() and companion != file_path:
                    companions.append(companion)
                    
        return companions

    @staticmethod
    def load(schema_path: Path) -> 'DatasetComponentSchema':
        """Load schema from file.
        
        Args:
            schema_path: Path to schema file
            
        Returns:
            Loaded schema
        """
        schema = DatasetComponentSchema(schema_path.parent.parent)
        schema.schema_path = schema_path
        schema.schema = schema._load()
        return schema
    
    def _load(self) -> Dict[str, Any]:
        """Load schema from file or create with defaults."""
        if self.schema_path.exists():
            with open(self.schema_path) as f:
                return json.load(f)
        return self._create_default_schema()
    
    def _create_default_schema(self) -> Dict[str, Any]:
        """Create default schema."""
        return {
            "version": "1.0",
            "components": {
                "instrumental": {
                    "pattern": "*_instrumental.mp3",
                    "required": True,
                    "multiple": False
                },
                "mir": {
                    "pattern": "*.mir.json",
                    "required": False,
                    "multiple": False
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
    
    def add_component(self, name: str, pattern: str, required: bool = False, multiple: bool = False) -> ValidationResult:
        """Add a new component to the schema.
        
        Args:
            name: Component name
            pattern: File pattern
            required: Whether component is required
            multiple: Whether multiple files are allowed
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], stats={
            "total_files": 0,
            "matched_files": 0,
            "unmatched_files": 0,
            "component_coverage": {}
        })

        # Validate component name
        if ' ' in name:
            result.is_valid = False
            result.errors.append(f"Invalid component name '{name}': component names cannot contain spaces")
            return result

        # Check if component already exists
        if name in self.schema["components"]:
            result.is_valid = False
            result.errors.append(f"Component {name} already exists")
            return result

        # Check for pattern collision with other components
        for comp_name, comp_info in self.schema["components"].items():
            if comp_info["pattern"] == pattern:
                result.is_valid = False
                result.errors.append(f"Pattern collision between components: {pattern}")
                return result

        # Add component
        self.schema["components"][name] = {
            "pattern": pattern,
            "required": required,
            "multiple": multiple
        }
        
        # Initialize component coverage
        result.stats["component_coverage"][name] = {
            "matched": 0,
            "unmatched": 0,
            "total_tracks": 0,
            "tracks_with_component": 0
        }
        
        return result
    
    def remove_component(self, name: str) -> ValidationResult:
        """Remove a component from the schema.
        
        Args:
            name: Component name
            
        Returns:
            Validation result
        """
        # Check if component exists
        if name not in self.schema["components"]:
            return ValidationResult(is_valid=False, errors=[f"Component {name} does not exist"], warnings=[], stats={})

        # Check if component is required
        if self.schema["components"][name]["required"]:
            return ValidationResult(is_valid=False, errors=[f"Cannot remove required component {name}"], warnings=[], stats={})

        # Remove component
        del self.schema["components"][name]
        
        # Validate after removing
        return self.validate()
    
    def validate_against_data(self, target_path: Optional[Path] = None) -> ValidationResult:
        """Validate schema against dataset files.
        
        Args:
            target_path: Optional specific path to validate against. If None, validates against entire dataset.
        
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], stats={
            "total_files": 0,
            "matched_files": 0,
            "unmatched_files": 0,
            "component_coverage": {},
            "directory_structure": {
                "artists": 0,
                "albums": 0,
                "cds": 0,
                "tracks": 0
            }
        })
        
        # Initialize component coverage
        for comp_name in self.schema["components"]:
            result.stats["component_coverage"][comp_name] = {
                "matched": 0,
                "unmatched": 0,
                "total_tracks": 0,
                "tracks_with_component": 0
            }
        
        # Group files by track
        track_files = defaultdict(list)
        total_files = 0
        
        base_path = target_path if target_path else self.dataset_path
        for root, _, files in os.walk(base_path):
            try:
                rel_path = Path(root).relative_to(self.dataset_path)
            except ValueError:
                    continue
                
            # Skip .blackbird directory
            if '.blackbird' in rel_path.parts:
                continue
            
            for file_name in files:
                file_path = rel_path / file_name
                base_name = Path(file_name).stem
                
                # First, try to match the file against component patterns
                matched_component = None
                for comp_name, comp_info in self.schema["components"].items():
                    pattern = comp_info["pattern"]
                    if fnmatch.fnmatch(file_name, pattern):
                        matched_component = comp_info
                        break
                    else:
                        # Check for versioned variants
                        pattern_base = pattern.rsplit(".", 1)[0] if "." in pattern else pattern
                        pattern_ext = pattern.rsplit(".", 1)[1] if "." in pattern else ""
                        versioned_pattern = f"{pattern_base}_*{pattern_ext}"
                        if fnmatch.fnmatch(file_name, versioned_pattern):
                            matched_component = comp_info
                            break
                
                # If we found a matching component, extract the base track name
                if matched_component:
                    pattern = matched_component["pattern"]
                    pattern_base = pattern.rsplit(".", 1)[0] if "." in pattern else pattern
                    if pattern_base.startswith("*"):
                        suffix = pattern_base[1:]
                        # Remove version suffix if present
                        if "_v" in base_name:
                            base_name = base_name[:base_name.rindex("_v")]
                        # Remove component suffix
                        if base_name.endswith(suffix):
                            base_name = base_name[:-len(suffix)]
                
                track_files[str(rel_path / base_name)].append(str(file_path))
                total_files += 1
        
        result.stats["total_files"] = total_files
        
        # Update total tracks count
        for comp_name in self.schema["components"]:
            result.stats["component_coverage"][comp_name]["total_tracks"] = len(track_files)
        
        # Check each track against schema
        matched_files = set()
        
        for track_path, files in track_files.items():
            for comp_name, comp_spec in self.schema["components"].items():
                pattern = comp_spec["pattern"]
                required = comp_spec["required"]
                multiple = comp_spec["multiple"]
                
                # Find all files matching the component pattern
                matches = []
                for file_path in files:
                    file_name = Path(file_path).name
                    # Check if file matches the pattern or any versioned variant
                    if fnmatch.fnmatch(file_name, pattern):
                        matches.append(file_path)
                    else:
                        # Check for versioned variants
                        pattern_base = pattern.rsplit(".", 1)[0] if "." in pattern else pattern
                        pattern_ext = pattern.rsplit(".", 1)[1] if "." in pattern else ""
                        versioned_pattern = f"{pattern_base}_v*{pattern_ext}"
                        if fnmatch.fnmatch(file_name, versioned_pattern):
                            matches.append(file_path)
                
                if matches:
                    matched_files.update(matches)
                    result.stats["component_coverage"][comp_name]["matched"] += len(matches)
                    result.stats["component_coverage"][comp_name]["tracks_with_component"] += 1
                
                if required and not matches:
                    result.add_error(f"Required component '{comp_name}' missing for track {track_path}")
                elif not multiple and len(matches) > 1:
                    result.add_error(f"Component '{comp_name}' has multiple files for track {track_path}")
            
            # Update unmatched files count for each component
            for comp_name in self.schema["components"]:
                result.stats["component_coverage"][comp_name]["unmatched"] = total_files - result.stats["component_coverage"][comp_name]["matched"]
        
        result.stats["matched_files"] = len(matched_files)
        result.stats["unmatched_files"] = total_files - len(matched_files)
        
        return result

    def get_track_relative_path(self, file_path: Union[str, Path]) -> str:
        """Get relative path for track from file path.
        
        Args:
            file_path: Path to track file
            
        Returns:
            Relative path
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
            
        try:
            relative_path = file_path.relative_to(self.dataset_path)
        except ValueError:
            # If file_path is already relative, use it as is
            relative_path = file_path
            
        # Remove component suffix and extension
        base_path = relative_path.parent / relative_path.stem
        for pattern in self.schema["components"].values():
            pattern_base = Path(pattern["pattern"]).stem
            if pattern_base.startswith("*"):
                suffix = pattern_base[1:]
                if base_path.name.endswith(suffix):
                    base_path = base_path.parent / base_path.name[:-len(suffix)]
                    break
                    
        return str(base_path)

    def _list_tracks(self, dataset_path: str) -> List[str]:
        """List all tracks in the dataset."""
        tracks = set()
        for root, _, files in os.walk(dataset_path):
            try:
                rel_path = Path(root).relative_to(dataset_path)
            except ValueError:
                continue
                
            # Skip .blackbird directory
            if '.blackbird' in rel_path.parts:
                continue
                
            # Group files by base name
            files_by_base = defaultdict(list)
            for file_name in files:
                base_name = Path(file_name).stem.split('_')[0]
                files_by_base[base_name].append(file_name)
                
            # Add each base name as a track
            for base_name in files_by_base:
                track_path = str(rel_path / base_name)
                tracks.add(track_path)
                
        return sorted(list(tracks))

    def _list_track_files(self, track_path: str) -> List[str]:
        """List all files for a track."""
        track_files = []
        track_dir = os.path.dirname(track_path)
        track_base = os.path.basename(track_path)
        
        # List all files in the track directory
        for file_name in os.listdir(os.path.join(self.dataset_path, track_dir)):
            if file_name.startswith(track_base + '_'):
                track_files.append(os.path.join(track_dir, file_name))
                
        return sorted(track_files)