from pathlib import Path
import json
from typing import Dict, Any, List, Optional, Union, Tuple, Set
from dataclasses import dataclass
import fnmatch
import pickle
import os
from collections import defaultdict
import re

class SchemaDiscoveryResult:
    """Result of schema discovery."""

    def __init__(self, is_valid: bool, stats: Optional[Dict[str, Any]] = None) -> None:
        """Initialize schema discovery result.

        Args:
            is_valid: Whether the schema discovery was successful
            stats: Optional statistics about discovered components
        """
        self.is_valid = is_valid
        self.stats = stats or {}

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
        seen_patterns = {}  # pattern -> component_name mapping
        for comp_name, comp_info in self.schema["components"].items():
            pattern = comp_info["pattern"]
            if pattern in seen_patterns:
                result.add_error(f"Pattern collision between components '{comp_name}' and '{seen_patterns[pattern]}': {pattern}")
                return result  # Return early if we find a collision
            seen_patterns[pattern] = comp_name
        
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

    def discover_schema(self, folders: Optional[List[str]] = None) -> SchemaDiscoveryResult:
        """Discover schema by analyzing the dataset.

        Args:
            folders: Optional list of folders to analyze. If not provided,
                    analyzes the entire dataset.

        Returns:
            SchemaDiscoveryResult indicating success and containing any errors
        """
        print(f"\nStarting discover_schema with folders: {folders}")
        
        # Reset schema for discovery
        self.schema = {
            "components": {},
            "structure": {},
            "sync": {}
        }

        # If folders are specified, analyze each folder, otherwise analyze the entire dataset
        if folders:
            all_postfix_groups = defaultdict(lambda: defaultdict(set))
            all_base_names = set()
            all_unmatched = set()
            
            for folder in folders:
                folder_path = self.dataset_path / folder
                if not folder_path.exists():
                    print(f"Warning: Folder {folder_path} does not exist")
                    continue
                    
                postfix_groups, base_names, unmatched = self._analyze_file_patterns_in_directory(str(folder_path))
                
                # Merge results
                for postfix, tracks in postfix_groups.items():
                    for base_name, files in tracks.items():
                        all_postfix_groups[postfix][base_name].update(files)
                all_base_names.update(base_names)
                all_unmatched.update(unmatched)
        else:
            # Analyze entire dataset
            all_postfix_groups, all_base_names, all_unmatched = self._analyze_file_patterns_in_directory(str(self.dataset_path))

        # Prepare stats for the result
        stats = {
            "total_files": sum(len(files) for tracks in all_postfix_groups.values() for files in tracks.values()),
            "base_names": len(all_base_names),
            "unmatched_files": len(all_unmatched),
            "components": {}
        }

        # Convert postfix analysis into schema components
        for postfix, tracks in all_postfix_groups.items():
            # Skip empty postfixes
            if not postfix:
                continue

            # Determine component name and pattern
            if postfix.startswith('_'):
                # For postfixes starting with underscore, extract name and extension
                base_part = postfix[1:].split('.')[0]  # Get part between _ and first .
                ext = ''.join(postfix.split('.')[1:])  # Get all extensions combined
                
                # Check if this is a numbered section pattern
                if re.search(r"\d+$", base_part):
                    # For numbered sections, use * to match any number
                    base_without_number = re.sub(r"\d+$", "", base_part)
                    component_name = f"{base_without_number}*.{ext}"
                    pattern = f"*_{base_without_number}*.{ext}"
                else:
                    # For regular components, keep the extension in the component name
                    component_name = f"{base_part}.{ext}"
                    pattern = f"*_{base_part}.{ext}"
            else:
                # For files without underscore prefix (like .mir.json),
                # use the full extension (including dots) as component name
                component_name = postfix.lstrip('.')  # Remove leading dot but keep internal dots
                pattern = f"*{postfix}"

            # Handle numbered sections - check for second asterisk before extension
            is_multiple = pattern.count('*') > 1

            # Add component to schema
            self.schema["components"][component_name] = {
                "pattern": pattern,
                "multiple": is_multiple,
                "description": ""  # Empty description by default
            }

            # Calculate track coverage using full paths
            track_paths = {file 
                         for base_name, files in tracks.items() 
                         for file in files}
            all_track_paths = {file 
                             for base_name in all_base_names 
                             for file in all_postfix_groups[postfix][base_name]}
            
            total_tracks = len(all_track_paths)
            tracks_with_component = len(track_paths)
            track_coverage = tracks_with_component / total_tracks if total_tracks > 0 else 0.0

            # Count total files, handling multiple-file components correctly
            total_files = sum(len(files) for files in tracks.values())

            # Add component stats
            stats["components"][component_name] = {
                "pattern": pattern,
                "track_count": len(track_paths),
                "file_count": total_files,
                "is_multiple": is_multiple,
                "track_coverage": track_coverage,
                "has_sections": is_multiple,
                "unique_tracks": tracks_with_component,
                "multiple": is_multiple
            }


        return SchemaDiscoveryResult(is_valid=True, stats=stats)

    def _find_base_name(self, filename: str) -> Optional[str]:
        """Find base name from a single filename.
        
        Args:
            filename: Name of file to analyze
            
        Returns:
            Base name if found, None otherwise
        """
        # Special case for compound extensions
        if filename.endswith('.mir.json'):
            return filename[:-9]  # Remove .mir.json

        # Get everything before first underscore or last dot
        if '_' in filename:
            return filename.split('_')[0]
        return filename.rsplit('.', 1)[0]

    def _extract_postfix(self, filename: str, base_name: str) -> Tuple[str, bool]:
        """Extract postfix from filename using the known base name.
        
        Args:
            filename: Name of file to analyze
            base_name: Known base name of the file
            
        Returns:
            Tuple of (postfix, is_numbered) where:
            - postfix is the extracted pattern
            - is_numbered indicates if this is a numbered section pattern
        """
        # Remove base name to get the postfix part
        postfix = filename[len(base_name):]
        
        # Check for numbered section pattern - look for numbers before the extension
        section_match = re.search(r"(_.+?)(\d+)([.][^.]+)$", postfix)
        if section_match:
            return (postfix, True)
        
        # Return the full postfix (includes leading underscore if present)
        return (postfix, False)

    def _analyze_file_patterns_in_directory(self, directory_path: str) -> Tuple[Dict[str, Dict[str, Set[str]]], Set[str], Set[str]]:
        """Analyze files in a directory to discover components.
        
        Examines all files in the directory and its subdirectories to identify:
        - Base names (track identifiers)
        - Postfix patterns (component identifiers)
        - Numbered section patterns
        - Special cases like compound extensions
        
        Args:
            directory_path: Path to directory to analyze
            
        Returns:
            Tuple of:
            - Dict mapping postfixes to Dict of base names to sets of files
            - Set of base names
            - Set of unmatched files
        """
        path = Path(directory_path)
        if not path.exists() or not path.is_dir():
            return defaultdict(lambda: defaultdict(set)), set(), set()

        # Collect all files with their relative paths
        all_files = set()
        for file_path in path.rglob('*'):
            if file_path.is_file() and '.blackbird' not in str(file_path):
                rel_path = str(file_path.relative_to(path))
                all_files.add(rel_path)

        # Group files by potential base names
        base_name_files = defaultdict(set)
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            base = self._find_base_name(file_name)
            if base:
                base_name_files[base].add(file_path)

        # Analyze postfixes for each base name
        postfix_groups = defaultdict(lambda: defaultdict(set))
        unmatched_files = set(all_files)
        base_names = set()

        for base_name, files in base_name_files.items():
            # Verify all files in this group have filenames starting with base_name
            if not all(os.path.basename(f).startswith(base_name) for f in files):
                continue

            base_names.add(base_name)

            # First pass: identify numbered components and their base patterns
            numbered_patterns = {}  # base pattern -> set of numbered files
            for file_path in files:
                file_name = os.path.basename(file_path)
                postfix, is_numbered = self._extract_postfix(file_name, base_name)
                if is_numbered:
                    # Extract base pattern by removing the number
                    section_match = re.search(r"(_.+?)(\d+)([.][^.]+)$", postfix)
                    if section_match:
                        base_pattern = section_match.group(1) + '*' + section_match.group(3)
                        if base_pattern not in numbered_patterns:
                            numbered_patterns[base_pattern] = set()
                        numbered_patterns[base_pattern].add(file_path)

            # Second pass: group files by postfix
            for file_path in files:
                file_name = os.path.basename(file_path)
                postfix, is_numbered = self._extract_postfix(file_name, base_name)
                
                if is_numbered:
                    # For numbered files, use the base pattern
                    section_match = re.search(r"(_.+?)(\d+)([.][^.]+)$", postfix)
                    if section_match:
                        base_pattern = section_match.group(1) + '*' + section_match.group(3)
                        postfix_groups[base_pattern][base_name].update(numbered_patterns[base_pattern])
                else:
                    # For regular files, use the exact postfix
                    postfix_groups[postfix][base_name].add(file_path)
                
                unmatched_files.discard(file_path)

        return postfix_groups, base_names, unmatched_files

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
            "components": {}  # Empty components dictionary
        }
    
    def add_component(self, name: str, pattern: str, multiple: bool = False) -> ValidationResult:
        """Add a new component to the schema.
        
        Args:
            name: Component name/identifier
            pattern: Glob pattern for matching files
            multiple: Whether multiple files of this type are allowed per track
            
        Returns:
            ValidationResult with validation status and any errors/warnings
        """
        # Validate pattern
        if not pattern:
            return ValidationResult(
                is_valid=False,
                errors=["Pattern cannot be empty"],
                warnings=[],
                stats={}
            )
        
        # Check for pattern collisions
        for existing_name, existing_config in self.schema["components"].items():
            if existing_config["pattern"] == pattern:
                # If the component already exists with the same pattern, just update it
                if existing_name == name:
                    self.schema["components"][name]["multiple"] = multiple
                    return self.validate()
                # Otherwise, it's a collision
                return ValidationResult(
                    is_valid=False,
                    errors=[f"Pattern collision with existing component '{existing_name}': {pattern}"],
                    warnings=[],
                    stats={}
                )
        
        # Add component
        self.schema["components"][name] = {
            "pattern": pattern,
            "multiple": multiple,
            "description": ""  # Empty description by default
        }
        
        return self.validate()

    def remove_component(self, name: str) -> ValidationResult:
        """Remove a component from the schema.
        
        Args:
            name: Name of component to remove
            
        Returns:
            ValidationResult with validation status and any errors/warnings
        """
        if name not in self.schema["components"]:
            return ValidationResult(
                is_valid=False,
                errors=[f"Component {name} not found in schema"],
                warnings=[],
                stats={}
            )
        
        # Remove the component
        del self.schema["components"][name]
        return self.validate()

    def validate_against_data(self, path: Optional[Path] = None) -> ValidationResult:
        """Validate the schema against the dataset."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            stats={
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
            }
        )
        
        # First validate the schema structure and check for pattern collisions
        self._validate_structure(result)
        if not result.is_valid:
            return result
        
        # If no path provided, use the dataset path
        validate_path = path if path else self.dataset_path
        
        # Initialize component coverage tracking
        component_coverage = {
            component: {
                "matched": 0,
                "unmatched": 0
            }
            for component in self.schema["components"]
        }
        
        # Track files by base name to check multiple files constraint
        track_components = defaultdict(lambda: defaultdict(list))  # base_name -> component -> list of files
        track_files = defaultdict(list)  # base_name -> list of files
        
        # First pass: collect all files and identify tracks
        for root, _, files in os.walk(validate_path):
            # Skip .blackbird directory
            if '.blackbird' in Path(root).parts:
                continue
                
            for filename in files:
                if filename.startswith('.'):
                    continue
                    
                file_path = os.path.join(root, filename)
                base_name = Path(filename).stem.split('_')[0]
                track_files[base_name].append(filename)
                result.stats["total_files"] += 1
                
                # Try to match file against component patterns
                matched = False
                for component, config in self.schema["components"].items():
                    if fnmatch.fnmatch(filename, config["pattern"]):
                        matched = True
                        result.stats["matched_files"] += 1
                        component_coverage[component]["matched"] += 1
                        track_components[base_name][component].append(filename)
                        break
                
                if not matched:
                    result.stats["unmatched_files"] += 1
                    result.add_warning(f"Unmatched file: {file_path}")
        
        # Second pass: check constraints for all tracks that have any files
        for base_name, files in track_files.items():
            # Check for multiple files constraint
            for component, config in self.schema["components"].items():
                if component in track_components[base_name] and not config["multiple"] and len(track_components[base_name][component]) > 1:
                    result.add_error(
                        f"Component '{component}' has multiple files for track '{base_name}' "
                        f"but multiple files are not allowed: {', '.join(track_components[base_name][component])}"
                    )
                    result.is_valid = False
        
        # Update component coverage statistics
        result.stats["component_coverage"] = component_coverage
        
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
        """List all tracks in the dataset by finding instrumental files."""
        print(f"\nListing tracks in {dataset_path}")
        tracks = set()
        for root, _, files in os.walk(dataset_path):
            try:
                rel_path = Path(root).relative_to(dataset_path)
            except ValueError:
                print(f"Skipping root in _list_tracks: {root}")
                continue
                
            # Skip .blackbird directory
            if '.blackbird' in rel_path.parts:
                print(f"Skipping .blackbird directory in _list_tracks: {rel_path}")
                continue
                
            # Look for instrumental files to identify tracks
            for file_name in files:
                if '_instrumental.mp3' in file_name:
                    # Get base name by removing _instrumental.mp3
                    base_name = file_name.replace('_instrumental.mp3', '')
                    track_path = str(rel_path / base_name)
                    print(f"Found track: {track_path}")
                    tracks.add(track_path)
                
        print(f"Total tracks found: {len(tracks)}")
        return sorted(list(tracks))

    def _list_track_files(self, track_path: str) -> List[str]:
        """List all files for a track."""
        track_files = []
        track_dir = os.path.dirname(track_path)
        track_base = os.path.basename(track_path)
        
        # List all files in the track directory
        for file_name in os.listdir(os.path.join(self.dataset_path, track_dir)):
            # Check if the file belongs to this track
            if file_name.startswith(track_base):
                # Check if it matches any component pattern
                for comp_info in self.schema["components"].values():
                    pattern = comp_info["pattern"]
                    if pattern.startswith('*'):
                        # Convert glob pattern to regex pattern
                        regex_pattern = f"^{track_base}{pattern[1:]}$"
                        if re.match(regex_pattern, file_name):
                            track_files.append(os.path.join(track_dir, file_name))
                            break
                
        return sorted(track_files)

    def parse_real_folder_and_report(self, folder_path: Union[str, Path]) -> None:
        """Parse a real folder using the current schema and display component files.
        
        This is a utility function to help understand how files in a folder
        map to the schema components.
        
        Args:
            folder_path: Path to the folder to analyze
        """
        folder_path = Path(folder_path)
        if not folder_path.exists():
            print(f"Error: Folder {folder_path} does not exist")
            return
            
        # Group files by component
        component_files = defaultdict(list)
        unmatched_files = []
        
        # Walk through all files
        for file_path in folder_path.rglob('*'):
            if not file_path.is_file() or '.blackbird' in str(file_path):
                continue
                
            # Try to match file against component patterns
            matched = False
            for comp_name, comp_info in self.schema["components"].items():
                pattern = comp_info["pattern"]
                if pattern.startswith('*'):
                    # Convert glob pattern to regex for exact matching
                    regex_pattern = f"^.*{pattern[1:].replace('*', '.*')}$"
                    if re.match(regex_pattern, file_path.name):
                        rel_path = file_path.relative_to(folder_path)
                        component_files[comp_name].append(str(rel_path))
                        matched = True
                        break
                        
            if not matched:
                rel_path = file_path.relative_to(folder_path)
                unmatched_files.append(str(rel_path))
                
        # Print report
        print(f"\nAnalysis of folder: {folder_path}")
        print("\nFiles by component:")
        
        for comp_name, files in sorted(component_files.items()):
            comp_info = self.schema["components"][comp_name]
            print(f"\n{comp_name}:")
            print(f"  Pattern: {comp_info['pattern']}")
            print(f"  Multiple: {comp_info['multiple']}")
            print(f"  Files ({len(files)}):")
            for file in sorted(files):
                print(f"    {file}")
                
        if unmatched_files:
            print("\nUnmatched files:")
            for file in sorted(unmatched_files):
                print(f"  {file}")
        else:
            print("\nAll files matched to components.")
            
        # Print summary
        total_files = sum(len(files) for files in component_files.values()) + len(unmatched_files)
        print(f"\nSummary:")
        print(f"  Total files: {total_files}")
        print(f"  Matched to components: {total_files - len(unmatched_files)}")
        print(f"  Unmatched: {len(unmatched_files)}")
        print(f"  Components used: {len(component_files)}")

    def _validate_structure(self, result: ValidationResult) -> None:
        """Validate the schema structure and check for pattern collisions.
        
        Args:
            result: Validation result to update
        """
        # Check for pattern collisions between components
        seen_patterns = {}  # pattern -> component_name
        for component_name, config in self.schema["components"].items():
            pattern = config["pattern"]
            if pattern in seen_patterns:
                result.add_error(
                    f"Pattern collision detected: Components '{component_name}' and "
                    f"'{seen_patterns[pattern]}' share the same pattern '{pattern}'"
                )
            seen_patterns[pattern] = component_name