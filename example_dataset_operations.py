#!/usr/bin/env python3
from pathlib import Path
from blackbird.dataset import Dataset
from blackbird.schema import DatasetComponentSchema
import json
from tabulate import tabulate
import sys

def print_section(title):
    """Print a section header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")

def format_size(size_bytes):
    """Format size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def show_progress(msg: str):
    """Show progress message."""
    print(f"\r{msg}", file=sys.stderr, end="")
    sys.stderr.flush()

def main():
    # Initialize dataset with real path
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    
    print_section("Creating Dataset Schema")
    # Create schema with all components
    schema = DatasetComponentSchema.create(dataset_path)
    schema.schema["components"].update({
        "vocals": {
            "pattern": "*_vocals_noreverb.mp3",
            "required": False,
            "description": "Isolated vocals without reverb"
        },
        "lyrics": {
            "pattern": "*_vocals_noreverb.json",
            "required": False,
            "description": "Lyrics and timing information"
        },
        "mir.json": {
            "pattern": "*.mir.json",
            "required": False,
            "description": "Music Information Retrieval analysis data"
        },
        "sections": {
            "pattern": "*_vocals_stretched_120bpm_section*.mp3",
            "required": False,
            "multiple": True,
            "description": "Cut sections of vocals stretched to 120 BPM"
        }
    })
    schema.save()
    print("Schema created at:", schema.schema_path)
    
    # Now initialize the dataset with the schema
    dataset = Dataset(dataset_path)
    
    print_section("Dataset Schema")
    # Show current schema
    with open(dataset.schema.schema_path) as f:
        schema = json.load(f)
    print("Components:")
    for name, config in schema["components"].items():
        print(f"- {name}:")
        for key, value in config.items():
            print(f"    {key}: {value}")
            
    # Validate dataset structure
    print_section("Dataset Validation")
    validation = dataset.validate()
    print(f"Valid: {validation.is_valid}")
    if validation.errors:
        print("\nErrors:")
        for error in validation.errors:
            print(f"- {error}")
    if validation.warnings:
        print("\nWarnings:")
        for warning in validation.warnings:
            print(f"- {warning}")
            
    # Show some statistics
    print_section("Dataset Statistics")
    print("Analyzing dataset (this may take a while)...")
    stats = dataset.analyze(progress_callback=show_progress)
    print("\nAnalysis complete!\n")  # New line after progress
    
    print(f"Total tracks: {stats['tracks']['total']}")
    print(f"Total size: {format_size(stats['total_size'])}")
    print(f"\nComponent coverage:")
    for component, count in stats["components"].items():
        percentage = (count / stats["tracks"]["total"]) * 100
        print(f"- {component}: {count} tracks ({percentage:.1f}%)")
        
    print(f"\nTop 5 artists by track count:")
    top_artists = sorted(
        stats["tracks"]["by_artist"].items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    for artist, count in top_artists:
        albums = stats["albums"][artist]
        print(f"- {artist}: {count} tracks across {len(albums)} albums")
        
    # Find some specific tracks
    print_section("Track Search Examples")
    
    # 1. Find tracks with all components
    print("Finding tracks with all components...")
    complete_tracks = dataset.find_tracks(
        has=["instrumental", "vocals", "mir.json"],
        progress_callback=show_progress
    )
    print("\nSearch complete!")
    print(f"Tracks with all components: {len(complete_tracks)}")
    if complete_tracks:
        print("\nExample complete track:")
        example_track = next(iter(complete_tracks.items()))
        print(f"Track: {example_track[0]}")
        print("Files:")
        for f in sorted(example_track[1]):
            print(f"- {f.name}")
            
    # 2. Find tracks missing vocals
    print("\nFinding tracks missing vocals...")
    missing_vocals = dataset.find_tracks(
        missing=["vocals"],
        progress_callback=show_progress
    )
    print("\nSearch complete!")
    print(f"Tracks missing vocals: {len(missing_vocals)}")
    
    # 3. Find CD albums
    print("\nFinding CD albums...")
    cd_tracks = []
    for track_id in dataset.find_tracks(progress_callback=show_progress).keys():
        if "/CD" in track_id:
            cd_tracks.append(track_id)
    print("\nSearch complete!")
    
    print(f"\nFound {len(cd_tracks)} tracks in CD albums")
    if cd_tracks:
        print("\nExample CD albums:")
        seen_albums = set()
        for track in sorted(cd_tracks):
            album_path = str(Path(track).parent.parent)
            if album_path not in seen_albums and len(seen_albums) < 3:
                print(f"- {album_path}")
                seen_albums.add(album_path)

if __name__ == "__main__":
    main() 