import os
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, List
from tqdm import tqdm
import logging
import re
from itertools import islice

# Set up logging to file for detailed report
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataset_analysis.log'),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

class DatasetAnalyzer:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.artists = set()                       
        self.albums = defaultdict(set)             
        self.total_size = 0
        
        # Track counts
        self.total_instrumental_tracks = 0
        self.tracks_with_vocals = 0
        self.tracks_with_mir = 0
        self.tracks_with_sections = 0
        self.total_sections = 0
        
        # Track details
        self.tracks_info = {}  # Store detailed info about each track
        self.section_counts = defaultdict(int)  # Distribution of section counts
        
    def _get_base_name(self, filename: str) -> str:
        """Extract base name from any variant of the file"""
        # Remove common suffixes
        for suffix in ['_instrumental.mp3', '_vocals_noreverb.mp3', '_vocals_noreverb.json', 
                      '.mir.json', '_vocals_stretched_120bpm_section']:
            if suffix in filename:
                return filename[:filename.index(suffix)]
        return filename
        
    def _analyze_file(self, filepath: Path, relative_path: Path):
        """Analyze a single file and update statistics"""
        try:
            # Update size statistics
            size = filepath.stat().st_size
            self.total_size += size
            
            # Extract artist and album info from path
            parts = relative_path.parts
            if len(parts) >= 2:  # At least artist/album structure
                artist = parts[0]
                album = parts[1]
                self.artists.add(artist)
                self.albums[artist].add(album)
                
                filename = filepath.name
                base_name = self._get_base_name(filename)
                track_key = str(filepath.parent / base_name)
                
                # Initialize track info if not exists
                if track_key not in self.tracks_info:
                    self.tracks_info[track_key] = {
                        'has_instrumental': False,
                        'has_vocals': False,
                        'has_mir': False,
                        'section_count': 0,
                        'artist': artist,
                        'album': album,
                        'base_name': base_name
                    }
                
                # Update track information
                if filename.endswith('_instrumental.mp3'):
                    self.total_instrumental_tracks += 1
                    self.tracks_info[track_key]['has_instrumental'] = True
                    
                elif filename.endswith('_vocals_noreverb.mp3'):
                    self.tracks_info[track_key]['has_vocals'] = True
                    
                elif filename.endswith('.mir.json'):
                    self.tracks_info[track_key]['has_mir'] = True
                    
                elif '_vocals_stretched_120bpm_section' in filename and filename.endswith('.mp3'):
                    self.tracks_info[track_key]['section_count'] += 1
                    self.total_sections += 1
                    
        except Exception as e:
            logger.error(f"Error analyzing file {filepath}: {str(e)}")

    def analyze(self):
        """Analyze the entire dataset structure"""
        print("Phase 1/2: Counting files...")
        total_files = sum(len(files) for _, _, files in os.walk(self.base_dir))
        
        print(f"\nPhase 2/2: Analyzing {total_files} files...")
        with tqdm(total=total_files, desc="Progress") as pbar:
            for root, _, files in os.walk(self.base_dir):
                root_path = Path(root)
                for file in files:
                    filepath = root_path / file
                    relative_path = filepath.relative_to(self.base_dir)
                    self._analyze_file(filepath, relative_path)
                    pbar.update(1)
        
        # Calculate summary statistics
        for track_info in self.tracks_info.values():
            if track_info['has_vocals']:
                self.tracks_with_vocals += 1
            if track_info['has_mir']:
                self.tracks_with_mir += 1
            if track_info['section_count'] > 0:
                self.tracks_with_sections += 1
                self.section_counts[track_info['section_count']] += 1
                    
    def print_summary(self):
        """Print a comprehensive summary with examples"""
        print("\n=== Dataset Summary ===")
        print(f"Total size: {self.total_size / (1024*1024*1024):.2f} GB")
        print(f"Total artists: {len(self.artists)}")
        print(f"Total albums: {sum(len(albums) for albums in self.albums.values())}")
        print(f"Total instrumental tracks: {self.total_instrumental_tracks}")
        
        if self.total_instrumental_tracks > 0:
            print("\n=== Component Coverage ===")
            print(f"Tracks with vocals: {self.tracks_with_vocals} ({(self.tracks_with_vocals/self.total_instrumental_tracks*100):.1f}%)")
            print(f"Tracks with MIR data: {self.tracks_with_mir} ({(self.tracks_with_mir/self.total_instrumental_tracks*100):.1f}%)")
            print(f"Tracks with sections: {self.tracks_with_sections} ({(self.tracks_with_sections/self.total_instrumental_tracks*100):.1f}%)")
            print(f"Total cut sections: {self.total_sections}")
            
            if self.tracks_with_sections > 0:
                avg_sections = self.total_sections / self.tracks_with_sections
                print(f"Average sections per track (for tracks with sections): {avg_sections:.1f}")
                
                print("\nSection count distribution:")
                for count in sorted(self.section_counts.keys()):
                    tracks = self.section_counts[count]
                    percentage = (tracks / self.tracks_with_sections) * 100
                    print(f"  {count} sections: {tracks} tracks ({percentage:.1f}%)")
        
        # Artist statistics
        print("\n=== Top Artists ===")
        artist_track_counts = defaultdict(int)
        for track_info in self.tracks_info.values():
            if track_info['has_instrumental']:
                artist_track_counts[track_info['artist']] += 1
                
        print("Top 5 artists by track count:")
        top_artists = sorted(artist_track_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for artist, count in top_artists:
            albums_count = len(self.albums[artist])
            print(f"- {artist}: {count} tracks across {albums_count} albums")
            
        print(f"\nDetailed report written to: dataset_analysis.log")
        
        # Write additional statistics to log file
        logger.info("\n=== Detailed Track Analysis ===")
        logger.info("\nSample of tracks with all components:")
        complete_tracks = [k for k, v in self.tracks_info.items() 
                         if v['has_instrumental'] and v['has_vocals'] and 
                         v['has_mir'] and v['section_count'] > 0]
        for track in islice(complete_tracks, 5):
            info = self.tracks_info[track]
            logger.info(f"\n{info['artist']} - {info['base_name']}")
            logger.info(f"Sections: {info['section_count']}")

def main():
    # Use the path from your dataset
    dataset_path = "/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb"
    
    analyzer = DatasetAnalyzer(dataset_path)
    analyzer.analyze()
    analyzer.print_summary()

if __name__ == "__main__":
    main() 