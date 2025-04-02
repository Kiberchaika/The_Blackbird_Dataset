from pathlib import Path
from typing import Dict, Set, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
import pickle
import logging
from collections import defaultdict
import time
import os
from tqdm import tqdm
import re
from difflib import get_close_matches

from .locations import LocationsManager # Added import

if TYPE_CHECKING:
    from .schema import DatasetComponentSchema

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
    stats_by_location: Dict[str, Dict] = field(default_factory=dict)  # location_name -> {file_count, total_size, track_count, album_count, artist_count}
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
        """Build a new index for the dataset scanning across all configured locations."""
        index = cls.create()
        location_roots: Dict[str, Path] = {}
        try:
            locations_manager = LocationsManager(dataset_path)
            # Now call load_locations and assign the result
            locations_dict = locations_manager.load_locations()
            # location_roots map names to resolved Path objects
            location_roots = locations_dict
            logger.info(f"Loaded {len(location_roots)} locations: {list(location_roots.keys())}")
        except Exception as e:
            logger.error(f"Failed to load locations from {dataset_path / '.blackbird' / 'locations.json'}: {e}. Assuming single 'Main' location at dataset root: {dataset_path}")
            resolved_root = dataset_path.resolve()
            location_roots = {"Main": resolved_root}

        # Initialize stats containers
        location_file_counts = {name: 0 for name in location_roots}
        location_total_sizes = {name: 0 for name in location_roots}
        location_artists = {name: set() for name in location_roots}
        location_albums = {name: set() for name in location_roots}
        location_tracks = {name: set() for name in location_roots}

        logger.info("Finding all files across locations...")
        found_count = 0
        # List of tuples: (abs_path, location_name, comp_name, base_name, size)
        matched_files_info = []
        unmatched_files = [] # Stores symbolic paths of unmatched files
        total_dirs = 0
        start_time = time.time()

        # Precompile patterns and prepare suffix list for base name extraction
        patterns_to_remove = []
        component_patterns = {}
        for comp_name, comp_info in schema.schema["components"].items():
            pattern = comp_info["pattern"]
            # Escape regex special chars and replace glob *
            regex_safe_pattern = pattern.replace(".", "\.").replace("*", ".*")
            component_patterns[comp_name] = re.compile(regex_safe_pattern + "$")
            # Identify potential suffix to remove for base name calculation
            # Assumes suffix starts after the first '*' or from the beginning if no '*'
            suffix_part = pattern
            if '*' in pattern:
                # Take the part after the first glob star as the potential suffix
                suffix_part = pattern.split('*', 1)[1]
                # Handle cases like *_vocals_*.mp3 -> _vocals_*.mp3 as suffix part
                # The goal is to remove the distinguishing part of the component pattern
            if suffix_part:
                patterns_to_remove.append(suffix_part)

        # Sort patterns by length descending to remove longest match first
        patterns_to_remove.sort(key=len, reverse=True)

        # First pass: Scan all locations and collect file info
        logger.info("Counting directories across all locations...")
        effective_locations_count = 0
        for location_name, location_root in location_roots.items():
            if not location_root.is_dir():
                logger.warning(f"Location '{location_name}' path '{location_root}' does not exist or is not a directory. Skipping count.")
                continue
            effective_locations_count += 1
            try:
                # Count directories, excluding .blackbird if it exists within this location (unlikely but possible)
                for root, dirs, _ in os.walk(location_root, topdown=True):
                    # Check if .blackbird is directly under this location_root
                    if Path(root) == location_root and '.blackbird' in dirs:
                        dirs.remove('.blackbird') # Prevent counting/walking into .blackbird

                    # Check if current root is inside a potential .blackbird dir (should be prevented by above, but belt-and-suspenders)
                    if '.blackbird' in Path(root).relative_to(location_root).parts:
                         dirs[:] = [] # Don't count or recurse into .blackbird
                         continue # Skip this dir count

                    total_dirs += (1 + len(dirs)) # Count current dir and subdirs found
            except Exception as e:
                 logger.warning(f"Error counting directories in {location_name} ({location_root}): {e}")

        logger.info(f"Found {total_dirs} total directories to scan across {effective_locations_count} accessible locations.")

        with tqdm(total=total_dirs, desc="Scanning directories", unit="dir") as pbar:
            for location_name, location_root in location_roots.items():
                if not location_root.is_dir():
                    # Already warned during count phase
                    continue # Skip scanning this location

                try:
                    for root, dirs, files in os.walk(location_root, topdown=True):
                         pbar.update(1) # Update progress per directory visited
                         current_root_path = Path(root)

                         # Prevent recursing into .blackbird
                         if '.blackbird' in dirs:
                             dirs.remove('.blackbird')
                         # Also skip processing if we somehow entered a .blackbird dir
                         if '.blackbird' in current_root_path.relative_to(location_root).parts:
                             continue

                         for filename in files:
                             # Skip hidden files (e.g., .DS_Store) and temp files
                             if filename.startswith('.') or filename.endswith(('.tmp', '.bak')):
                                 continue

                             abs_path = current_root_path / filename
                             found_count += 1
                             file_matched = False

                             try:
                                 # Check against component patterns
                                 for comp_name, regex_pattern in component_patterns.items():
                                     if regex_pattern.search(filename):
                                         # Calculate base_name by removing the longest matching component suffix
                                         base_name = filename
                                         for suffix in patterns_to_remove:
                                             # Attempt to remove suffix pattern from the end
                                             # Needs careful handling of glob * within suffix
                                             escaped_suffix_regex = suffix.replace(".", "\.").replace("*", ".*")
                                             match = re.search(f"^(.*?)({escaped_suffix_regex})$", base_name)
                                             if match:
                                                 potential_base = match.group(1)
                                                 # Check if removing suffix resulted in empty string or just '_'
                                                 if potential_base and potential_base != '_':
                                                    base_name = potential_base
                                                    break # Removed the longest suffix, stop.
                                                 # Else: suffix removal was too aggressive, try next shorter suffix

                                         # Final cleanup - remove trailing underscores and any remaining extension
                                         base_name = base_name.rstrip('_')
                                         base_name = Path(base_name).stem

                                         if not base_name:
                                             logger.warning(f"Could not determine base name for file: {filename} in {location_name}. Skipping.")
                                             continue

                                         size = abs_path.stat().st_size
                                         matched_files_info.append((abs_path, location_name, comp_name, base_name, size))
                                         file_matched = True
                                         break # Stop after first component match for this file

                                 if not file_matched:
                                     # Store symbolic path for unmatched files
                                     rel_path_unmatched = abs_path.relative_to(location_root)
                                     symbolic_unmatched = f"{location_name}/{rel_path_unmatched}"
                                     unmatched_files.append(symbolic_unmatched)

                             except FileNotFoundError:
                                 logger.warning(f"File vanished during scan: {abs_path}. Skipping.")
                                 found_count -=1 # Adjust count
                             except OSError as e:
                                 logger.error(f"OS error accessing file {abs_path}: {e}. Skipping.")
                                 found_count -= 1 # Adjust count
                             except ValueError as e: # Catch potential relative_to errors
                                logger.warning(f"Path calculation error for {abs_path} in {location_name}: {e}. Skipping unmatched.")
                             except Exception as e:
                                logger.error(f"Unexpected error matching file {filename} in {location_name}: {e}")

                         # Update postfix less frequently for performance if needed
                         if pbar.n % 50 == 0: # Update every 50 dirs
                              pbar.set_postfix({"loc": location_name[:10], "files": found_count, "matched": len(matched_files_info)}, refresh=False)

                except Exception as e:
                    logger.error(f"Error scanning location {location_name} ({location_root}): {e}")
                    # How to update pbar if a whole location fails? Difficult to estimate dirs. Log and continue.

            # Ensure final postfix update reflects the end state
            pbar.set_postfix({"files": found_count, "matched": len(matched_files_info)}, refresh=True)

        elapsed = time.time() - start_time
        rate = found_count / elapsed if elapsed > 0 else 0
        logger.info(f"Scanning summary:")
        logger.info(f"Locations scanned: {effective_locations_count}")
        logger.info(f"Total files found: {found_count}")
        logger.info(f"Files matched to components: {len(matched_files_info)}")
        logger.info(f"Unmatched files: {len(unmatched_files)}")
        logger.info(f"Scanning rate: {rate:.0f} files/sec")

        # Second pass: Group files by track and create index entries using symbolic paths
        logger.info("Creating tracks from matched files...")
        # Group by (location_name, artist, album, cd_number, base_name) which defines a track instance
        # Key: (loc, artist, album, cd, base) -> Value: List of (comp_name, abs_path, size)
        tracks_grouped = defaultdict(list)

        component_counts = defaultdict(int)
        component_sizes = defaultdict(int)

        with tqdm(total=len(matched_files_info), desc="Grouping files", unit="file") as pbar:
            for abs_path, location_name, comp_name, base_name, size in matched_files_info:
                pbar.update(1)
                try:
                    location_root = location_roots[location_name]
                    rel_path_in_loc = abs_path.relative_to(location_root)
                    parts = rel_path_in_loc.parent.parts

                    # Validate structure: Artist/Album/[CDx]/track_base_comp.ext
                    if not parts: # File directly in location root
                        logger.warning(f"Skipping file in location root (needs Artist/Album structure): {location_name}/{rel_path_in_loc}")
                        continue
                    artist = parts[0]
                    if len(parts) < 2: # Must have at least Artist/Album
                        logger.warning(f"Skipping file (needs Artist/Album structure): {location_name}/{rel_path_in_loc}")
                        continue
                    album = parts[1]

                    cd_number = None
                    expected_parent_parts = 2 # Artist/Album
                    # Check for standard CD structure: Artist/Album/CDX/...
                    if len(parts) >= 3 and parts[2].startswith('CD') and parts[2][2:].isdigit():
                        cd_number = parts[2]
                        expected_parent_parts = 3 # Artist/Album/CDX

                    # Ensure the file is directly within the expected level (Artist/Album or Artist/Album/CDX)
                    if len(parts) != expected_parent_parts:
                        logger.warning(f"Skipping file with unexpected directory structure: {location_name}/{rel_path_in_loc}")
                        continue

                    # Define the unique key for this track instance in this location
                    track_key = (location_name, artist, album, cd_number, base_name)
                    tracks_grouped[track_key].append((comp_name, abs_path, size))

                    # Aggregate stats (will be stored per-location later if needed)
                    component_counts[comp_name] += 1
                    component_sizes[comp_name] += size
                    index.total_size += size # Aggregate total size for now

                except ValueError as e:
                    logger.warning(f"Path structure error for {abs_path} relative to {location_root}: {e}. Skipping grouping.")
                except KeyError:
                    logger.error(f"Location name '{location_name}' mismatch for path {abs_path} during grouping. This shouldn't happen.")
                except Exception as e:
                    logger.error(f"Unexpected error grouping file {abs_path}: {e}")

        logger.info(f"Grouped matched files into {len(tracks_grouped)} unique track instances.")

        # Create TrackInfo objects and populate index using symbolic paths
        with tqdm(total=len(tracks_grouped), desc="Creating index entries", unit="track") as pbar:
            for (location_name, artist, album, cd_number, base_name), components_info in tracks_grouped.items():
                pbar.update(1)
                # Construct symbolic paths
                # Album path part relative to location root
                symbolic_album_relative = f"{artist}/{album}"
                # Full symbolic album path including location name
                symbolic_album_path = f"{location_name}/{symbolic_album_relative}"

                # Track path part relative to location root (including CD if present)
                symbolic_track_relative = symbolic_album_relative
                if cd_number:
                    symbolic_track_relative += f"/{cd_number}"
                symbolic_track_relative += f"/{base_name}"

                # This is the main identifier for the track *instance* in the index
                # Full symbolic track path including location name
                symbolic_track_path = f"{location_name}/{symbolic_track_relative}"

                # Create TrackInfo
                track = TrackInfo(
                    track_path=symbolic_track_path, # Key: full symbolic path to the track base in its location
                    artist=artist,
                    album_path=symbolic_album_path, # Store symbolic album path (Location/Artist/Album)
                    cd_number=cd_number,
                    base_name=base_name,
                    files={}, # component_name -> symbolic_file_path
                    file_sizes={} # symbolic_file_path -> size
                )

                # Add component files with their symbolic paths
                all_components_added = True
                for comp_name, abs_path, size in components_info:
                    try:
                        rel_path_in_loc = abs_path.relative_to(location_roots[location_name])
                        symbolic_file_path = f"{location_name}/{rel_path_in_loc}"
                        track.files[comp_name] = symbolic_file_path
                        track.file_sizes[symbolic_file_path] = size
                    except ValueError:
                        logger.warning(f"Could not form relative path for {abs_path} in {location_name}. Skipping component {comp_name} for track {symbolic_track_path}")
                        all_components_added = False
                    except KeyError:
                        logger.error(f"Location name '{location_name}' mismatch for path {abs_path} during TrackInfo creation.")
                        all_components_added = False

                # Only add track to index if all its found components could be processed
                if all_components_added and track.files:
                    # Update index lookups with symbolic paths
                    if symbolic_track_path in index.tracks:
                        logger.warning(f"Duplicate symbolic track path detected: {symbolic_track_path}. Overwriting entry. This indicates a track base name exists in multiple locations, or data duplication.")
                    index.tracks[symbolic_track_path] = track
                    index.track_by_album.setdefault(symbolic_album_path, set()).add(symbolic_track_path)
                    index.album_by_artist.setdefault(artist, set()).add(symbolic_album_path)
                elif not track.files:
                     logger.debug(f"Track {symbolic_track_path} had no processable components, not adding to index.")
                else:
                    logger.warning(f"Track {symbolic_track_path} skipped due to errors processing components.")

                if progress_callback:
                    if pbar.total > 0:
                         progress_callback(pbar.n / pbar.total)

                # Update final per-location stats (track, album, artist counts)
                location_tracks[location_name].add(symbolic_track_path)
                location_albums[location_name].add(symbolic_album_path)
                location_artists[location_name].add(artist)

        # Print detailed indexing results
        logger.info("Final Indexing Results:")
        logger.info(f"Locations processed: {effective_locations_count} / {len(location_roots)}")
        logger.info(f"Total track instances indexed: {len(index.tracks)}")
        logger.info(f"Total unique artists: {len(index.album_by_artist)}")
        # Count unique albums across locations by stripping location prefix for counting
        # An album is defined by Artist/Album pair, ignoring location for this count
        unique_albums = set()
        if index.album_by_artist:
             unique_albums = set(ap.split('/', 1)[1] for ap_set in index.album_by_artist.values() for ap in ap_set if '/' in ap)

        logger.info(f"Total unique albums: {len(unique_albums)}")
        logger.info(f"Total indexed size: {index.total_size / (1024**3):.2f} GB")

        logger.info("Components indexed (aggregated across locations):")
        # Sort components for consistent output
        total_indexed_files = 0
        if schema.schema["components"]:
            for comp_name in sorted(schema.schema["components"].keys()):
                count = component_counts.get(comp_name, 0)
                total_indexed_files += count
                size_gb = component_sizes.get(comp_name, 0) / (1024**3)
                comp_pattern = schema.schema['components'][comp_name].get('pattern', 'N/A')
                logger.info(f"  - {comp_name} ({comp_pattern}): {count} files ({size_gb:.2f} GB)")
        else:
            logger.info("  No components defined in schema.")

        logger.info(f"Total component files indexed: {total_indexed_files}")

        if len(unmatched_files) > 0:
            logger.info(f"Found {len(unmatched_files)} unmatched files.")
            logger.info("Sample of unmatched files (first 10):")
            # Sort unmatched for consistent output
            for f in sorted(unmatched_files)[:10]:
                logger.info(f"  {f}")

        # Assemble the stats_by_location dictionary
        for loc_name in location_roots.keys():
             index.stats_by_location[loc_name] = {
                 "file_count": location_file_counts[loc_name],
                 "total_size": location_total_sizes[loc_name],
                 "track_count": len(location_tracks[loc_name]),
                 "album_count": len(location_albums[loc_name]),
                 "artist_count": len(location_artists[loc_name]),
             }

        index.last_updated = datetime.now()
        logger.info("Index build complete.")
        return index