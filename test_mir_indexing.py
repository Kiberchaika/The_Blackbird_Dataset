#!/usr/bin/env python3

from pathlib import Path
import os
from collections import defaultdict
import logging
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.schema import DatasetComponentSchema
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def index_mir_files(dataset_path: Path) -> DatasetIndex:
    """Index only .mir.json files, bypassing schema complexity."""
    
    # Load schema to get correct component name
    schema = DatasetComponentSchema.load(dataset_path / ".blackbird" / "schema.json")
    mir_component_name = next(name for name, info in schema.schema["components"].items() 
                            if info["pattern"] == "*.mir.json")
    logger.info(f"Using component name from schema: {mir_component_name}")
    
    index = DatasetIndex(
        last_updated=datetime.now(),
        tracks={},
        track_by_album={},
        album_by_artist={},
        total_size=0
    )
    
    # Group files by their base name within each directory
    companion_lookup = defaultdict(lambda: defaultdict(dict))
    
    # Find all .mir.json files
    logger.info("Finding .mir.json files...")
    for root, _, files in os.walk(dataset_path):
        for filename in files:
            if not filename.endswith('.mir.json'):
                continue
                
            file_path = Path(root) / filename
            rel_path = file_path.relative_to(dataset_path)
            
            # Extract path components
            parts = rel_path.parts
            if len(parts) < 3:  # Need at least artist/album/file
                continue
                
            artist = parts[0]
            album = parts[1]
            album_path = str(Path(artist) / album)
            
            # Get base name (remove .mir.json)
            base_name = filename.replace('.mir.json', '')
            
            # Create track path
            track_path = str(Path(album_path) / base_name)
            
            # Create track info
            track = TrackInfo(
                track_path=track_path,
                artist=artist,
                album_path=album_path,
                cd_number=None,
                base_name=base_name,
                files={mir_component_name: str(rel_path)},  # Use component name from schema
                file_sizes={str(rel_path): file_path.stat().st_size}
            )
            
            # Update index
            index.tracks[track_path] = track
            index.track_by_album.setdefault(album_path, set()).add(track_path)
            index.album_by_artist.setdefault(artist, set()).add(album_path)
            index.total_size += file_path.stat().st_size
            
            logger.debug(f"Indexed: {rel_path}")
    
    return index

def main():
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_Part1")
    
    logger.info(f"Indexing .mir.json files in {dataset_path}")
    index = index_mir_files(dataset_path)
    
    # Print summary
    print(f"\nIndexing Summary:")
    print(f"Total tracks with .mir.json: {len(index.tracks)}")
    print(f"Total artists: {len(index.album_by_artist)}")
    print(f"Total size: {index.total_size / (1024*1024):.2f} MB")
    
    # Print sample of indexed files
    print("\nSample of indexed tracks:")
    for i, (track_path, track) in enumerate(list(index.tracks.items())[:5]):
        print(f"\n{i+1}. Track: {track_path}")
        print(f"   Artist: {track.artist}")
        print(f"   Album: {track.album_path}")
        print(f"   MIR file: {track.files[next(iter(track.files))]}")  # Use first component name

if __name__ == "__main__":
    main() 