from pathlib import Path
from blackbird.schema import DatasetComponentSchema
import re
from typing import List

def find_cd_album(dataset_path: Path) -> tuple[Path, Path]:
    """Find an album with CDs and a regular album for comparison."""
    cd_album = None
    regular_album = None
    
    for artist_dir in dataset_path.iterdir():
        if not artist_dir.is_dir():
            continue
            
        for album_dir in artist_dir.iterdir():
            if not album_dir.is_dir():
                continue
                
            # Check if this album has CD subdirectories
            cd_dirs = [d for d in album_dir.iterdir() 
                      if d.is_dir() and re.match(r'CD\d+', d.name)]
            
            if cd_dirs and not cd_album:
                cd_album = album_dir
            elif not cd_dirs and not regular_album:
                # Check if it has any instrumental tracks
                if list(album_dir.rglob("*_instrumental.mp3")):
                    regular_album = album_dir
                    
            if cd_album and regular_album:
                break
                
        if cd_album and regular_album:
            break
            
    return cd_album, regular_album

def print_track_details(schema: DatasetComponentSchema, track_path: Path):
    """Print detailed information about a track and its files."""
    print("\n=== Track Details ===")
    print(f"Track file: {track_path}")
    
    # Show how we identify this track in different ways
    print("\nIdentification:")
    print(f"1. Base name (for finding companion files in same dir):")
    print(f"   {schema._get_base_name(track_path.name)}")
    print(f"2. Relative path (unique track identifier in dataset):")
    print(f"   {schema.get_track_relative_path(track_path)}")
    
    # Show all companion files
    companions = schema.find_companion_files(track_path)
    print(f"\nCompanion files in same directory ({len(companions)} total):")
    for f in sorted(companions):
        if f == track_path:
            print(f"   {f.name} (this file)")
        else:
            print(f"   {f.name}")

def analyze_album(schema: DatasetComponentSchema, album_path: Path, title: str):
    """Analyze and print information about an album."""
    print(f"\n{title}")
    print(f"Album path: {album_path}")
    
    # Find all instrumental tracks
    tracks = list(album_path.rglob("*_instrumental.mp3"))
    print(f"\nFound {len(tracks)} tracks")
    
    # Group tracks by their relative paths
    track_groups = schema._group_by_track(tracks)
    print(f"Grouped into {len(track_groups)} unique tracks")
    
    # Print detailed info for first track
    if tracks:
        print("\nDetailed example for first track:")
        print_track_details(schema, tracks[0])
        
        # If this is a CD album, also show a track from another CD
        if len(tracks) > 1:
            cd1_track = tracks[0]
            other_cd_track = next(
                (t for t in tracks 
                 if schema.get_track_relative_path(t) != schema.get_track_relative_path(cd1_track)),
                None
            )
            if other_cd_track:
                print("\nComparison with track from different CD:")
                print_track_details(schema, other_cd_track)

def main():
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    schema = DatasetComponentSchema(dataset_path)
    
    cd_album, regular_album = find_cd_album(dataset_path)
    
    if cd_album:
        analyze_album(schema, cd_album, "=== Album with CDs ===")
    else:
        print("No album with CDs found!")
        
    if regular_album:
        analyze_album(schema, regular_album, "=== Regular Album ===")
    else:
        print("No regular album found!")

if __name__ == "__main__":
    main() 