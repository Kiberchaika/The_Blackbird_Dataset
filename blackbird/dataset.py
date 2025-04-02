from pathlib import Path
from typing import List, Dict, Set, Optional, Callable
from .schema import DatasetComponentSchema, ValidationResult
from .index import DatasetIndex, TrackInfo
import logging
from collections import defaultdict
from tqdm import tqdm
from .locations import LocationsManager
from .locations import resolve_symbolic_path, SymbolicPathError

logger = logging.getLogger(__name__)

class Dataset:
    """Main interface for dataset operations and management.
    
    This class serves as the primary interface for all dataset operations.
    It internally manages both the schema (component definitions) and index
    (efficient file lookup) to provide a unified interface for dataset operations.
    """
    
    def __init__(self, path: Path):
        """Initialize dataset with path.
        
        Args:
            path: Path to dataset root directory
        """
        self.path = Path(path).resolve()
        self.locations = LocationsManager(self.path)
        try:
            self.locations.load_locations()
        except Exception as e:
            logger.warning(f"Failed to load locations: {e}. Using default.")
        
        self._schema = DatasetComponentSchema(self.path)
        self._index = self._load_or_build_index()
        
    def _load_or_build_index(self) -> DatasetIndex:
        """Load existing index or build a new one if not found."""
        index_path = self.path / ".blackbird" / "index.pickle"
        if index_path.exists():
            try:
                return DatasetIndex.load(index_path)
            except Exception as e:
                logger.warning(f"Failed to load existing index at {index_path}: {e}. Rebuilding.")
        return self._rebuild_index()
        
    def _rebuild_index(self) -> DatasetIndex:
        """Build a fresh index of the dataset, considering all locations."""
        logger.info(f"Rebuilding dataset index across all locations...")
        
        if not self._schema.schema or not self._schema.schema.get('components'):
            logger.warning("Schema has no components defined. Loading schema from file if available.")
            schema_path = self.path / ".blackbird" / "schema.json"
            if schema_path.exists():
                try:
                    self._schema = DatasetComponentSchema.load(schema_path)
                    logger.info(f"Loaded schema from {schema_path}")
                except Exception as e:
                    logger.error(f"Failed to load schema from {schema_path}: {e}")
            else:
                logger.warning(f"Schema file {schema_path} not found.")

        logger.info(f"Schema components: {list(self._schema.schema.get('components', {}).keys())}")
        
        if not self._schema.schema.get('components'):
            logger.warning("No components defined in schema. Index will likely be incomplete.")
        
        index = DatasetIndex.build(self.path, self._schema, self.locations)
        
        component_counts = defaultdict(int)
        component_sizes = defaultdict(int)
        total_files = 0
        for track_path_symbolic, track_info in index.tracks.items():
            total_files += len(track_info.files)
            for comp_name, file_path_symbolic in track_info.files.items():
                component_counts[comp_name] += 1
                if file_path_symbolic in track_info.file_sizes:
                    component_sizes[comp_name] += track_info.file_sizes[file_path_symbolic]
                else:
                    logger.warning(f"Size missing for symbolic path '{file_path_symbolic}' in track '{track_path_symbolic}'. Stats might be inaccurate.")

        logger.info("\nIndex rebuilt successfully!")
        logger.info(f"\nNew index statistics (across all locations):")
        logger.info(f"Total tracks found: {len(index.tracks)}")
        logger.info(f"Total unique artists: {len(index.album_by_artist)}")
        logger.info(f"Total unique albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
        logger.info(f"Total files indexed: {total_files}")
        logger.info(f"\nComponents indexed:")
        for comp_name in sorted(component_counts.keys()):
            count = component_counts[comp_name]
            size_gb = component_sizes[comp_name] / (1024*1024*1024)
            logger.info(f"  {comp_name}: {count} files ({size_gb:.2f} GB)")
        
        index_path = self.path / ".blackbird" / "index.pickle"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index.save(index_path)
        logger.info(f"\nIndex saved to: {index_path}")
        
        return index
        
    def validate(self) -> ValidationResult:
        """Validate entire dataset against schema."""
        return self._schema.validate()
        
    def resolve_path(self, symbolic_path: str) -> Path:
        """Resolves a symbolic path (e.g., 'Main/Artist/Album/track.mp3') to an absolute path.

        Args:
            symbolic_path: The symbolic path string.

        Returns:
            The resolved absolute Path object.

        Raises:
            SymbolicPathError: If the path cannot be resolved.
            ValueError: If the input is invalid.
        """
        # Ensure locations are loaded
        current_locations = self.locations.get_all_locations()
        if not current_locations:
             # This might indicate an issue, but resolve_symbolic_path handles empty dict
             logger.warning("Attempting to resolve path with no locations loaded.")
             
        try:
            return resolve_symbolic_path(symbolic_path, current_locations)
        except (SymbolicPathError, ValueError) as e:
             logger.error(f"Failed to resolve symbolic path '{symbolic_path}': {e}")
             # Re-raise the original error to propagate it
             raise e 
        
    def find_tracks(
        self,
        has: Optional[List[str]] = None,
        missing: Optional[List[str]] = None,
        artist: Optional[str] = None,
        album: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, List[Path]]:
        """Find tracks based on component presence and metadata.
        
        Returns dictionary mapping symbolic track identifiers to lists of resolved *absolute* component file paths.
        """
        has = has or []
        missing = missing or []
        
        all_components = set(self._schema.schema.get("components", {}).keys())
        requested_components = set(has) | set(missing)
        invalid_components = requested_components - all_components
        if invalid_components:
            raise ValueError(f"Invalid or unknown components requested: {invalid_components}. Available: {all_components}")
            
        if progress_callback:
            progress_callback("Searching tracks in index...")
            
        matching_tracks: Dict[str, List[Path]] = {}
        tracks_to_check = self._index.search_by_track("", artist=artist, album=album)
        
        logger.debug(f"Index search returned {len(tracks_to_check)} potential tracks.")
        
        for track_info in tracks_to_check:
            track_components = set(track_info.files.keys())
            
            if all(c in track_components for c in has) and \
               all(c not in track_components for c in missing):
                
                resolved_file_paths: List[Path] = []
                for symbolic_file_path in track_info.files.values():
                    try:
                        resolved_path = self.resolve_path(symbolic_file_path)
                        resolved_file_paths.append(resolved_path)
                    except Exception as e:
                        logger.error(f"Error resolving path '{symbolic_file_path}' for track '{track_info.track_path}': {e}")
                        pass 
                        
                if resolved_file_paths:
                    matching_tracks[track_info.track_path] = resolved_file_paths
                else:
                    logger.warning(f"Skipping track '{track_info.track_path}' as no file paths could be resolved.")

        logger.debug(f"Found {len(matching_tracks)} final matching tracks.")
        return matching_tracks
        
    def analyze(self, progress_callback: Optional[Callable[[str], None]] = None) -> Dict:
        """Analyze dataset and return statistics using the index.
        
        Note: Statistics are based on the index, which uses symbolic paths.
        Size calculations rely on sizes stored during indexing.
        Per-location stats might be added later.
        """ 
        if progress_callback:
            progress_callback("Analyzing dataset from index...")
            
        stats = {
            "total_size": self._index.total_size,
            "artists": set(self._index.album_by_artist.keys()),
            "albums": {
                artist: {Path(album_path).name for album_path in albums}
                for artist, albums in self._index.album_by_artist.items()
            },
            "components": defaultdict(lambda: {'count': 0, 'size': 0}),
            "tracks": {
                "total": len(self._index.tracks),
                "complete": 0,
                "by_artist": defaultdict(int)
            }
        }
        
        all_schema_components = set(self._schema.schema.get("components", {}).keys())
        
        for track_path_symbolic, track_info in self._index.tracks.items():
            track_components = set(track_info.files.keys())
            
            for comp_name, file_path_symbolic in track_info.files.items():
                stats["components"][comp_name]['count'] += 1
                if file_path_symbolic in track_info.file_sizes:
                    stats["components"][comp_name]['size'] += track_info.file_sizes[file_path_symbolic]
                else:
                    logger.warning(f"Size missing for symbolic path '{file_path_symbolic}' in track '{track_path_symbolic}' during analysis.")

            stats["tracks"]["by_artist"][track_info.artist] += 1
            
            if track_components == all_schema_components:
                stats["tracks"]["complete"] += 1
                
        return stats
        
    def rebuild_index(self) -> None:
        """Force rebuild of the dataset index."""
        self._index = self._rebuild_index()
        
    @property
    def schema(self) -> DatasetComponentSchema:
        """Get the dataset schema (read-only)."""
        if not self._schema.schema:
            try:
                self._schema.load()
            except FileNotFoundError:
                logger.warning("Schema file not found on access.")
            except Exception as e:
                logger.error(f"Error loading schema on access: {e}")
        return self._schema
        
    @property 
    def index(self) -> DatasetIndex:
        """Get the dataset index (read-only)."""
        if not hasattr(self, '_index') or self._index is None:
            logger.warning("Index accessed before initialization, attempting load/build.")
            self._index = self._load_or_build_index()
        return self._index
