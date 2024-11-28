from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import json
import re
from collections import defaultdict

@dataclass
class Field:
    """Defines a field in the dataset with its file pattern and metadata"""
    name: str
    postfix: str  # e.g. "_vocals_stretched_120bpm_section*" or "_caption"
    description: str
    is_multi: bool = False  # Whether field can have multiple files (like sections)
    load_in_memory: bool = False  # Whether to load content in memory during initialization
    
    def matches_filename(self, filename: str) -> bool:
        """Check if filename matches this field's pattern"""
        pattern = self.postfix.replace('*', r'\d*')
        return bool(re.search(f"{pattern}\.", filename))
    
    def get_index_from_filename(self, filename: str) -> Optional[int]:
        """Extract index from filename for multi-fields"""
        if not self.is_multi:
            return None
        pattern = self.postfix.replace('*', r'(\d+)')
        match = re.search(pattern, filename)
        return int(match.group(1)) if match else None

@dataclass
class Track:
    """Represents a single track with all its associated files and metadata"""
    base_name: str
    artist: str
    album: Tuple[str, Optional[str]]  # (album_name, cd_number)
    
    # Paths to audio files (relative to dataset root)
    audio_paths: Dict[str, str] = field(default_factory=dict)
    
    # Fields that are loaded into memory
    bpm: Optional[float] = None
    lyrics: Optional[str] = None
    sections: List[Dict[str, Any]] = field(default_factory=list)
    
    # All available fields for this track
    available_fields: Set[str] = field(default_factory=set)

class MusicDataset:
    def __init__(self, root_path: str):
        self.root_path = Path(root_path)
        
        # Define all possible fields
        self.fields = {
            'ref': Field('ref', '_caption', 'Reference caption file', load_in_memory=True),
            'mir': Field('mir', '_preprocessed_chords.mir', 'Music information retrieval data', load_in_memory=True),
            'vocals': Field('vocals', '_vocal_only', 'Isolated vocals track'),
            'section': Field('section', '_vocals_stretched_120bpm_section*', 'Vocal section file', is_multi=True),
            'lyrics': Field('lyrics', '_lyrics', 'Lyrics file', load_in_memory=True)
        }
        
        # Storage for tracks
        self.tracks: Dict[str, Track] = {}
        self.artist_albums: Dict[str, Set[Tuple[str, Optional[str]]]] = defaultdict(set)
        
        # Initialize dataset
        self._scan_dataset()
        
        # Print statistics
        self._print_stats()
    
    def _parse_path(self, filepath: Path) -> Tuple[str, str, Optional[str], str]:
        """Parse path into artist, album, cd_number, and filename"""
        parts = filepath.relative_to(self.root_path).parts
        artist = parts[0]
        
        if len(parts) == 3:
            return artist, parts[1], None, parts[2]
        elif len(parts) == 4 and parts[2].startswith('CD'):
            return artist, parts[1], parts[2], parts[3]
        else:
            raise ValueError(f"Unexpected path structure: {filepath}")
    
    def _scan_dataset(self):
        """Scan the dataset directory and build the track database"""
        for filepath in self.root_path.rglob('*'):
            if not filepath.is_file():
                continue
                
            try:
                artist, album_name, cd_number, filename = self._parse_path(filepath)
            except ValueError:
                continue
            
            # Get base name by removing all known field postfixes
            base_name = filename
            for field in self.fields.values():
                base_name = re.sub(f"{field.postfix}\\.[^.]+$", '', base_name)
            
            # Create or get track
            track_id = f"{artist}/{album_name}/{cd_number if cd_number else ''}/{base_name}"
            if track_id not in self.tracks:
                self.tracks[track_id] = Track(
                    base_name=base_name,
                    artist=artist,
                    album=(album_name, cd_number)
                )
                self.artist_albums[artist].add((album_name, cd_number))
            
            track = self.tracks[track_id]
            
            # Process file according to its field
            for field_name, field in self.fields.items():
                if field.matches_filename(filename):
                    track.available_fields.add(field_name)
                    rel_path = str(filepath.relative_to(self.root_path))
                    
                    if field.is_multi:
                        idx = field.get_index_from_filename(filename)
                        if idx is not None:
                            track.audio_paths[f"{field_name}_{idx}"] = rel_path
                    else:
                        track.audio_paths[field_name] = rel_path
                        
                    # Load in-memory data
                    if field.load_in_memory:
                        if field_name == 'mir':
                            with open(filepath) as f:
                                mir_data = json.load(f)
                                track.bpm = mir_data.get('bpm')
                        elif field_name == 'lyrics':
                            track.lyrics = filepath.read_text()
                        elif field_name == 'section':
                            # Load section data
                            if filepath.suffix == '.json':
                                with open(filepath) as f:
                                    section_data = json.load(f)
                                    track.sections.append(section_data)
    
    def _print_stats(self):
        """Print dataset statistics"""
        total_tracks = len(self.tracks)
        field_counts = defaultdict(int)
        
        for track in self.tracks.values():
            for field in track.available_fields:
                field_counts[field] += 1
        
        print(f"\nDataset Statistics:")
        print(f"Total base files found: {total_tracks}")
        print("\nFiles per field type:")
        for field_name, count in field_counts.items():
            print(f"- {field_name}: {count} files ({count/total_tracks*100:.1f}%)")
    
    def get_refs(self) -> List[Track]:
        """Get all tracks that have reference files"""
        return [track for track in self.tracks.values() if 'ref' in track.available_fields]
    
    def get_artist_albums(self, artist: str) -> List[Tuple[str, Optional[str]]]:
        """Get all albums by an artist"""
        return sorted(list(self.artist_albums.get(artist, set())))
    
    def get_artist_tracks(self, artist: str) -> List[Track]:
        """Get all tracks by an artist"""
        return [track for track in self.tracks.values() if track.artist == artist]
    
    def get_album_tracks(self, artist: str, album_name: str, cd_number: Optional[str] = None) -> List[Track]:
        """Get all tracks from a specific album"""
        return [
            track for track in self.tracks.values()
            if track.artist == artist and track.album == (album_name, cd_number)
        ]

# Usage example:
if __name__ == "__main__":
    dataset = MusicDataset("/media/k4_nas/Datasets/Music_RU/Separated")
    
    # Get all reference tracks
    ref_tracks = dataset.get_refs()
    print(f"Found {len(ref_tracks)} reference tracks")
    
    # Get all albums by artist
    artist = "Artist Name"
    albums = dataset.get_artist_albums(artist)
    print(f"\nAlbums by {artist}:")
    for album_name, cd_number in albums:
        cd_str = f" (CD{cd_number})" if cd_number else ""
        print(f"- {album_name}{cd_str}")
        
        # Get tracks from this album
        album_tracks = dataset.get_album_tracks(artist, album_name, cd_number)
        for track in album_tracks:
            print(f"  - {track.base_name} (BPM: {track.bpm})")