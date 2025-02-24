from pathlib import Path
from typing import Dict, Set, List, Optional
from dataclasses import dataclass
from datetime import datetime
import pickle
import logging
from collections import defaultdict
import time
import os
from tqdm import tqdm
import re
from difflib import get_close_matches

logger = logging.getLogger(__name__)

@dataclass
class TrackInfo:
    """Track information in the index."""
    track_path: str      # Relative path identifying the track (artist/album/[cd]/track)
    artist: str         # Artist name
    album_path: str     # Full path to album (artist/album)
    cd_number: Optional[str]  # CD number if present
    base_name: str      # Track name without component suffixes
    files: Dict[str, str]  # component_name -> file_path mapping
    file_sizes: Dict[str, int]  # file_path -> size in bytes

@dataclass
class DatasetIndex:
    """Main index structure."""
    last_updated: datetime
    tracks: Dict[str, TrackInfo]  # track_path -> TrackInfo
    track_by_album: Dict[str, Set[str]]  # album_path -> set of track_paths
    album_by_artist: Dict[str, Set[str]]  # artist_name -> set of album_paths
    total_size: int  # Total size of all indexed files
    version: str = "1.0"  # Move version with default value to the end

    @classmethod
    def create(cls) -> 'DatasetIndex':
        """Create a new empty index."""
        return cls(
            last_updated=datetime.now(),
            tracks={},
            track_by_album={},
            album_by_artist={},
            total_size=0
        )

    def save(self, path: Path) -> None:
        """Save index to file."""
        path = Path(path)
        
        # Create backup of existing index if it exists
        if path.exists():
            backup_path = path.with_suffix('.bak')
            path.rename(backup_path)
        
        # Save directly to the target path
        with open(path, 'wb') as f:
            pickle.dump(self, f, protocol=5)

    @classmethod
    def load(cls, path: Path) -> 'DatasetIndex':
        """Load index from file."""
        with open(path, 'rb') as f:
            return pickle.load(f)

    def search_by_artist(self, query: str, case_sensitive: bool = False, fuzzy_search: bool = False) -> List[str]:
        """Search for artists matching the query.
        
        Args:
            query: Artist name query
            case_sensitive: Whether to perform case-sensitive matching
            fuzzy_search: Whether to use fuzzy matching for similar names when no exact matches are found
            
        Returns:
            List of artist names that contain the query string or are similar to it
        """
        # First try exact/substring matching
        if case_sensitive:
            matches = [artist for artist in self.album_by_artist.keys() 
                      if query in artist]
        else:
            query_lower = query.lower()
            matches = [artist for artist in self.album_by_artist.keys() 
                      if query_lower in artist.lower()]
        
        # If no matches found and fuzzy search is enabled, try fuzzy matching
        if not matches and fuzzy_search:
            artists = list(self.album_by_artist.keys())
            
            if not case_sensitive:
                # For case-insensitive search, convert query to lowercase
                query_lower = query.lower()
                # Create a mapping of lowercase to original names
                case_map = {artist.lower(): artist for artist in artists}
                # Get close matches using lowercase versions
                fuzzy_matches = get_close_matches(query_lower, list(case_map.keys()), n=5, cutoff=0.6)
                # Map back to original artist names
                matches = [case_map[match] for match in fuzzy_matches]
            else:
                # For case-sensitive search, use original strings
                matches = get_close_matches(query, artists, n=5, cutoff=0.6)
        
        return matches

    def search_by_album(self, album_query: str, artist: Optional[str] = None) -> List[str]:
        """Search for albums by name.

        Args:
            album_query: Album name query
            artist: Optional artist name to filter by

        Returns:
            List of album paths matching the query
        """
        # First filter by artist if specified
        albums_to_search = []
        if artist:
            if artist in self.album_by_artist:
                albums_to_search.extend(self.album_by_artist[artist])
        else:
            for artist_albums in self.album_by_artist.values():
                albums_to_search.extend(artist_albums)

        # Then filter by album name
        matches = []
        for album_path in albums_to_search:
            album_name = album_path.split('/')[-1]
            if album_query.lower() in album_name.lower():
                matches.append(album_path)

        return sorted(matches)  # Sort for consistent ordering

    def search_by_track(self, query: str, artist: Optional[str] = None, 
                       album: Optional[str] = None, case_sensitive: bool = False) -> List[TrackInfo]:
        """Search for tracks matching the query.
        If artist and/or album are provided, only search within those.
        Returns a list of TrackInfo objects that contain the query string."""
        if not case_sensitive:
            query = query.lower()
        
        results = []
        for track_path, track_info in self.tracks.items():
            # Apply artist filter if provided
            if artist and track_info.artist != artist:
                continue
            
            # Apply album filter if provided
            if album and track_info.album_path != album:
                continue
            
            # Check if query matches track name
            if (not case_sensitive and query in track_info.base_name.lower()) or \
               (case_sensitive and query in track_info.base_name):
                results.append(track_info)
        
        return results

    def get_track_files(self, track_path: str) -> Dict[str, str]:
        """Get all files associated with a track.
        Returns a dictionary mapping component names to file paths."""
        if track_path not in self.tracks:
            return {}
        return self.tracks[track_path].files

    @classmethod
    def build(cls, dataset_path: Path, schema: 'DatasetComponentSchema', progress_callback=None) -> 'DatasetIndex':
        """Build a new index for the dataset."""
        index = cls.create()
        
        # First count total directories for progress bar
        logger.info("Counting directories...")
        total_dirs = sum(1 for _ in os.walk(dataset_path))
        logger.info(f"Found {total_dirs} directories to scan")
        
        # Find all files with progress
        logger.info("Finding all files...")
        found_count = 0
        matched_count = 0
        start_time = time.time()
        
        # Track files by directory and base name
        track_files = defaultdict(lambda: defaultdict(dict))  # dir -> base_name -> comp_name -> (path, size)
        unmatched_files = []
        
        # Extract patterns to remove for getting base names
        patterns_to_remove = []
        for comp_name, comp_info in schema.schema["components"].items():
            pattern = comp_info["pattern"]
            if pattern.startswith("*"):
                patterns_to_remove.append(pattern[1:])
        
        with tqdm(total=total_dirs, desc="Scanning directories") as pbar:
            for root, _, files in os.walk(dataset_path):
                for filename in files:
                    if filename.startswith('.') or '.blackbird' in Path(root).parts:
                        continue
                        
                    file_path = Path(root) / filename
                    found_count += 1
                    
                    # Try to match file against component patterns
                    file_matched = False
                    for comp_name, comp_info in schema.schema["components"].items():
                        pattern = comp_info["pattern"]
                        regex_pattern = pattern.replace(".", "\\.").replace("*", ".*")
                        if re.search(regex_pattern + "$", filename):
                            rel_path = file_path.relative_to(dataset_path)
                            size = file_path.stat().st_size
                            
                            # Get base name by removing all component patterns
                            base_name = filename
                            for pattern in patterns_to_remove:
                                pattern_base = Path(pattern).stem
                                if pattern_base:
                                    base_name = base_name.replace(pattern_base, '')
                            base_name = Path(base_name).stem  # Remove extension
                            
                            # Store file info
                            dir_path = str(rel_path.parent)
                            track_files[dir_path][base_name][comp_name] = (rel_path, size)
                            matched_count += 1
                            file_matched = True
                            break
                    
                    if not file_matched and not filename.endswith(('.tmp', '.bak')):
                        unmatched_files.append(str(file_path.relative_to(dataset_path)))
                
                pbar.set_postfix({"files": found_count, "matched": matched_count}, refresh=True)
                pbar.update(1)
        
        elapsed = time.time() - start_time
        rate = found_count / elapsed if elapsed > 0 else 0
        logger.info(f"\nIndexing summary:")
        logger.info(f"Total files found: {found_count}")
        logger.info(f"Files matched to components: {matched_count}")
        logger.info(f"Unmatched files: {len(unmatched_files)}")
        logger.info(f"Processing rate: {rate:.0f} files/sec")
        
        # Create tracks from grouped files
        logger.info("\nCreating tracks from matched files...")
        component_counts = defaultdict(int)
        component_sizes = defaultdict(int)
        
        with tqdm(total=sum(len(base_names) for base_names in track_files.values()), desc="Creating tracks") as pbar:
            for dir_path, base_names in track_files.items():
                for base_name, components in base_names.items():
                    # Extract path components
                    parts = Path(dir_path).parts
                    artist = parts[0]
                    album = parts[1]
                    album_path = str(Path(artist) / album)
                    
                    cd_number = None
                    if len(parts) == 3 and parts[2].startswith('CD'):  # artist/album/cd
                        cd_number = parts[2]
                    
                    # Create track path
                    track_path = str(Path(album_path) / base_name)
                    if cd_number:
                        track_path = str(Path(album_path) / cd_number / base_name)
                    
                    # Create track info with all components
                    track = TrackInfo(
                        track_path=track_path,
                        artist=artist,
                        album_path=album_path,
                        cd_number=cd_number,
                        base_name=base_name,
                        files={},
                        file_sizes={}
                    )
                    
                    # Add all components
                    for comp_name, (rel_path, size) in components.items():
                        track.files[comp_name] = str(rel_path)
                        track.file_sizes[str(rel_path)] = size
                        index.total_size += size
                        component_counts[comp_name] += 1
                        component_sizes[comp_name] += size
                    
                    # Update index
                    index.tracks[track_path] = track
                    index.track_by_album.setdefault(album_path, set()).add(track_path)
                    index.album_by_artist.setdefault(artist, set()).add(album_path)
                    
                    pbar.update(1)
                    if progress_callback:
                        progress_callback(pbar.n / pbar.total)
        
        # Print detailed indexing results
        logger.info("\nIndexing Results:")
        logger.info(f"Total tracks indexed: {len(index.tracks)}")
        logger.info(f"Total artists: {len(index.album_by_artist)}")
        logger.info(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
        logger.info(f"Total size: {index.total_size / (1024*1024*1024):.2f} GB")
        
        logger.info("\nComponents indexed:")
        for comp_name in sorted(component_counts.keys()):
            count = component_counts[comp_name]
            size_gb = component_sizes[comp_name] / (1024*1024*1024)
            logger.info(f"  {comp_name}: {count} files ({size_gb:.2f} GB)")
        
        if len(unmatched_files) > 0:
            logger.info("\nSample of unmatched files (first 10):")
            for f in sorted(unmatched_files)[:10]:
                logger.info(f"  {f}")
        
        index.last_updated = datetime.now()
        return index 