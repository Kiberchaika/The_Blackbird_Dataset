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

    def search_by_artist(self, query: str, case_sensitive: bool = False) -> List[str]:
        """Search for artists matching the query.
        Returns a list of artist names that contain the query string."""
        if not case_sensitive:
            query = query.lower()
            return [artist for artist in self.album_by_artist.keys() 
                   if query in artist.lower()]
        return [artist for artist in self.album_by_artist.keys() 
               if query in artist]

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

def build_index(
    dataset_path: Path,
    schema: 'DatasetComponentSchema',
    progress_callback=None
) -> DatasetIndex:
    """Build a new index for the dataset."""
    index = DatasetIndex.create()
    file_groups = {
        comp_name: []
        for comp_name in schema.schema["components"]
    }
    
    # First count total directories for progress bar
    logger.info("Counting directories...")
    total_dirs = sum(1 for _ in os.walk(dataset_path))
    logger.info(f"Found {total_dirs} directories to scan")
    
    # Find all files with progress
    logger.info("Finding all files...")
    found_count = 0
    start_time = time.time()
    
    with tqdm(total=total_dirs, desc="Scanning directories") as pbar:
        for root, _, files in os.walk(dataset_path):
            for filename in files:
                file_path = Path(root) / filename
                found_count += 1
                
                # Group file by pattern
                for comp_name, comp_info in schema.schema["components"].items():
                    pattern = comp_info["pattern"]
                    if filename.endswith(pattern[1:]):  # Skip the * at the start
                        rel_path = file_path.relative_to(dataset_path)
                        size = file_path.stat().st_size
                        file_groups[comp_name].append((rel_path, size))
                        break  # File can only match one pattern
            
            # Update progress
            pbar.set_postfix({"files": found_count}, refresh=True)
            pbar.update(1)
    
    elapsed = time.time() - start_time
    rate = found_count / elapsed if elapsed > 0 else 0
    logger.info(f"Found {found_count} total files in {elapsed:.1f} seconds ({rate:.0f} files/sec)")
    
    for comp_name, files in file_groups.items():
        logger.info(f"  {comp_name}: {len(files)} files")
    
    # Create lookup for companion files by directory and base name
    t_lookup = time.time()
    companion_lookup = defaultdict(lambda: defaultdict(dict))
    for comp_name, files in file_groups.items():
        if comp_name == 'instrumental_audio':
            continue
        for rel_path, size in files:
            dir_path = str(rel_path.parent)
            base_name = rel_path.stem
            for pattern in ['_vocals_noreverb', '_instrumental']:
                base_name = base_name.replace(pattern, '')
            companion_lookup[dir_path][base_name][comp_name] = (rel_path, size)
    logger.info(f"Built companion lookup in {(time.time() - t_lookup) * 1000:.0f}ms")
    
    # Process instrumental files to build index
    instrumental_files = file_groups['instrumental_audio']
    logger.info(f"Processing {len(instrumental_files)} instrumental files...")
    
    # Process files with progress bar
    with tqdm(total=len(instrumental_files), desc="Building index") as pbar:
        for rel_path, size in instrumental_files:
            # Get path components (all string operations, no I/O)
            parts = rel_path.parts
            artist = parts[0]
            album = parts[1]
            album_path = str(Path(artist) / album)
            
            cd_number = None
            if len(parts) == 4:  # artist/album/cd/file
                cd_dir = parts[2]
                if cd_dir.startswith('CD'):
                    cd_number = cd_dir
            
            # Get base name (string operations)
            base_name = rel_path.stem.replace('_instrumental', '')
            dir_path = str(rel_path.parent)
            
            # Create track path
            track_path = str(Path(album_path) / base_name)
            if cd_number:
                track_path = str(Path(album_path) / cd_number / base_name)
            
            # Create track info
            track = TrackInfo(
                track_path=track_path,
                artist=artist,
                album_path=album_path,
                cd_number=cd_number,
                base_name=base_name,
                files={'instrumental_audio': str(rel_path)},
                file_sizes={str(rel_path): size}
            )
            
            # Find companion files using lookup (no I/O)
            if dir_path in companion_lookup and base_name in companion_lookup[dir_path]:
                for comp_name, (comp_path, comp_size) in companion_lookup[dir_path][base_name].items():
                    track.files[comp_name] = str(comp_path)
                    track.file_sizes[str(comp_path)] = comp_size
                    index.total_size += comp_size
            
            # Update index (memory operations only)
            index.tracks[track_path] = track
            index.track_by_album.setdefault(album_path, set()).add(track_path)
            index.album_by_artist.setdefault(artist, set()).add(album_path)
            index.total_size += size
            
            pbar.update(1)
            if progress_callback:
                progress_callback(pbar.n / pbar.total)
    
    index.last_updated = datetime.now()
    return index 