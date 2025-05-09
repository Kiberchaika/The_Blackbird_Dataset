from pathlib import Path
from typing import List, Dict, Set, Optional, Callable
from .schema import DatasetComponentSchema, ValidationResult
from .index import DatasetIndex, TrackInfo
import logging
from collections import defaultdict
from tqdm import tqdm

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
        self.path = Path(path)
        self._schema = DatasetComponentSchema(self.path)
        self._index = self._load_or_build_index()
        
    def _load_or_build_index(self) -> DatasetIndex:
        """Load existing index or build a new one if not found."""
        index_path = self.path / ".blackbird" / "index.pickle"
        if index_path.exists():
            return DatasetIndex.load(index_path)
        return self._rebuild_index()
        
    def _rebuild_index(self) -> DatasetIndex:
        """Build a fresh index of the dataset."""
        logger.info(f"Rebuilding dataset index...")
        
        # Make sure schema is loaded
        if not self._schema.schema or not self._schema.schema.get('components'):
            logger.warning("Schema has no components defined. Loading schema from file if available.")
            schema_path = self.path / ".blackbird" / "schema.json"
            if schema_path.exists():
                self._schema = DatasetComponentSchema.load(schema_path)
        
        logger.info(f"Schema components: {list(self._schema.schema.get('components', {}).keys())}")
        
        # If still no components, warn but continue
        if not self._schema.schema.get('components'):
            logger.warning("No components defined in schema. Index will be empty.")
        
        index = DatasetIndex.build(self.path, self._schema)
        
        # Calculate component statistics
        component_counts = defaultdict(int)
        component_sizes = defaultdict(int)
        for track in index.tracks.values():
            for comp_name, file_path in track.files.items():
                component_counts[comp_name] += 1
                component_sizes[comp_name] += track.file_sizes[file_path]
        
        # Print statistics
        logger.info("\nIndex rebuilt successfully!")
        logger.info(f"\nNew index statistics:")
        logger.info(f"Total tracks: {len(index.tracks)}")
        logger.info(f"Total artists: {len(index.album_by_artist)}")
        logger.info(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
        logger.info(f"\nComponents indexed:")
        for comp_name in sorted(component_counts.keys()):
            count = component_counts[comp_name]
            size_gb = component_sizes[comp_name] / (1024*1024*1024)
            logger.info(f"  {comp_name}: {count} files ({size_gb:.2f} GB)")
        
        # Save the index
        index_path = self.path / ".blackbird" / "index.pickle"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index.save(index_path)
        logger.info(f"\nIndex saved to: {index_path}")
        
        return index
        
    def validate(self) -> ValidationResult:
        """Validate entire dataset against schema."""
        return self._schema.validate()
        
    def find_tracks(
        self,
        has: Optional[List[str]] = None,
        missing: Optional[List[str]] = None,
        artist: Optional[str] = None,
        album: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, List[Path]]:
        """Find tracks based on component presence and metadata.
        
        This method uses the index for efficient lookups. First time usage on a 
        dataset will trigger index building.
        
        Args:
            has: List of components that must be present
            missing: List of components that must be missing
            artist: Filter by artist name
            album: Filter by album name
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary mapping track identifiers to lists of component files
        """
        has = has or []
        missing = missing or []
        
        # Validate component names against schema
        all_components = set(has) | set(missing)
        invalid_components = all_components - set(self._schema.schema["components"].keys())
        if invalid_components:
            raise ValueError(f"Invalid components: {invalid_components}")
            
        if progress_callback:
            progress_callback("Searching tracks in index...")
            
        # Use index to find matching tracks
        matching_tracks: Dict[str, List[Path]] = {}
        
        # First filter by artist/album if specified
        tracks_to_check = self._index.search_by_track("", artist=artist, album=album)
        
        for track_info in tracks_to_check:
            # Get component names present for this track
            track_components = set(track_info.files.keys())
            
            # Check component presence requirements
            if all(c in track_components for c in has) and \
               all(c not in track_components for c in missing):
                # Convert relative paths to absolute Path objects
                matching_tracks[track_info.track_path] = [
                    self.path / file_path
                    for file_path in track_info.files.values()
                ]
                
        return matching_tracks
        
    def analyze(self, progress_callback: Optional[Callable[[str], None]] = None) -> Dict:
        """Analyze dataset and return statistics using the index.
        
        Args:
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with various statistics about the dataset
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
            "components": defaultdict(int),
            "tracks": {
                "total": len(self._index.tracks),
                "complete": 0,
                "by_artist": defaultdict(int)
            }
        }
        
        # Analyze component presence and track completeness
        for track_info in self._index.tracks.values():
            # Count components
            for component in track_info.files:
                stats["components"][component] += 1
                
            # Update artist stats
            stats["tracks"]["by_artist"][track_info.artist] += 1
            
            # Check if track has all components
            if set(track_info.files.keys()) == set(self._schema.schema["components"].keys()):
                stats["tracks"]["complete"] += 1
                
        return stats
        
    def rebuild_index(self) -> None:
        """Force rebuild of the dataset index."""
        self._index = self._rebuild_index()
        
    @property
    def schema(self) -> DatasetComponentSchema:
        """Get the dataset schema (read-only)."""
        return self._schema
        
    @property 
    def index(self) -> DatasetIndex:
        """Get the dataset index (read-only)."""
        return self._index
