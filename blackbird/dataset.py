from pathlib import Path
from typing import List, Dict, Set, Optional, Callable
from .schema import DatasetComponentSchema, SchemaValidationResult
from collections import defaultdict
import logging
import fnmatch
from tqdm import tqdm

logger = logging.getLogger(__name__)

class Dataset:
    """Main class for dataset operations and management."""
    
    def __init__(self, path: Path):
        """Initialize dataset with path.
        
        Args:
            path: Path to dataset root directory
        """
        self.path = Path(path)
        self.schema = DatasetComponentSchema(self.path)
        
    def validate(self) -> SchemaValidationResult:
        """Validate entire dataset against schema.
        
        Returns:
            Validation result with statistics
        """
        return self.schema.validate()
        
    def find_tracks(
        self,
        has: Optional[List[str]] = None,
        missing: Optional[List[str]] = None,
        artist: Optional[str] = None,
        album: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Dict[str, List[Path]]:
        """Find tracks based on component presence and metadata.
        
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
        
        # Validate component names
        all_components = set(has) | set(missing)
        invalid_components = all_components - set(self.schema.schema["components"].keys())
        if invalid_components:
            raise ValueError(f"Invalid components: {invalid_components}")
            
        # Find all instrumental tracks as base set
        if progress_callback:
            progress_callback("Finding instrumental tracks...")
        base_tracks = list(self.path.rglob("*_instrumental.mp3"))
        track_groups = defaultdict(list)
        
        # Group tracks by their relative path
        if progress_callback:
            progress_callback(f"Processing {len(base_tracks)} tracks...")
        for track in tqdm(base_tracks, desc="Grouping tracks", disable=progress_callback is None):
            track_id = self.schema.get_track_relative_path(track)
            track_groups[track_id].append(track)
            
            # Add companion files
            companions = self.schema.find_companion_files(track)
            track_groups[track_id].extend([f for f in companions if f != track])
        
        # Filter by artist/album if specified
        if artist or album:
            if progress_callback:
                progress_callback("Applying artist/album filters...")
            filtered_groups = defaultdict(list)
            for track_id, files in track_groups.items():
                track_path = Path(track_id)
                parts = track_path.parts
                
                if artist and parts[0] != artist:
                    continue
                if album and parts[1] != album:
                    continue
                    
                filtered_groups[track_id].extend(files)
            track_groups = filtered_groups
            
        # Check component presence
        if progress_callback:
            progress_callback("Checking component presence...")
        result_tracks = defaultdict(list)
        for track_id, files in track_groups.items():
            # Check which components are present
            has_components = set()
            for component, config in self.schema.schema["components"].items():
                pattern = config["pattern"]
                if any(fnmatch.fnmatch(str(f.name), pattern) for f in files):
                    has_components.add(component)
                    
            # Apply filters
            if all(c in has_components for c in has) and \
               all(c not in has_components for c in missing):
                result_tracks[track_id].extend(files)
                
        return dict(result_tracks)
        
    def analyze(self, progress_callback: Optional[Callable[[str], None]] = None) -> Dict:
        """Analyze dataset and return statistics.
        
        Args:
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with various statistics about the dataset
        """
        stats = {
            "total_size": 0,
            "artists": set(),
            "albums": defaultdict(set),
            "components": defaultdict(int),
            "tracks": {
                "total": 0,
                "complete": 0,
                "by_artist": defaultdict(int)
            }
        }
        
        # Find all tracks
        if progress_callback:
            progress_callback("Finding and grouping all tracks...")
        all_tracks = self.find_tracks(progress_callback=progress_callback)
        stats["tracks"]["total"] = len(all_tracks)
        
        if progress_callback:
            progress_callback(f"Analyzing {len(all_tracks)} tracks...")
        
        for track_id, files in tqdm(all_tracks.items(), desc="Analyzing tracks", disable=progress_callback is None):
            # Update size
            stats["total_size"] += sum(f.stat().st_size for f in files)
            
            # Update artist/album info
            track_path = Path(track_id)
            parts = track_path.parts
            artist = parts[0]
            album = parts[1]
            stats["artists"].add(artist)
            stats["albums"][artist].add(album)
            stats["tracks"]["by_artist"][artist] += 1
            
            # Count components
            has_components = set()
            for component, config in self.schema.schema["components"].items():
                pattern = config["pattern"]
                if any(fnmatch.fnmatch(str(f.name), pattern) for f in files):
                    has_components.add(component)
                    stats["components"][component] += 1
                    
            # Check if track has all components
            if has_components == set(self.schema.schema["components"].keys()):
                stats["tracks"]["complete"] += 1
                
        # Convert sets to lists for JSON serialization
        if progress_callback:
            progress_callback("Finalizing statistics...")
        stats["artists"] = sorted(stats["artists"])
        stats["albums"] = {
            artist: sorted(albums) 
            for artist, albums in stats["albums"].items()
        }
        
        return stats
