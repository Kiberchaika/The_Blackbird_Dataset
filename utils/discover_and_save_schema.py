#!/usr/bin/env python3

import argparse
from pathlib import Path
import random
import json
from blackbird.schema import DatasetComponentSchema

def discover_and_save_schema(dataset_path: Path, num_artists: int = 25) -> None:
    """
    Discover and save schema for a dataset.
    
    Args:
        dataset_path: Path to the dataset root directory
        num_artists: Number of random artists to analyze (default: 25)
    """
    # Create schema manager
    schema = DatasetComponentSchema(dataset_path)
    
    # Get all artist folders
    artist_folders = [f for f in dataset_path.iterdir() if f.is_dir() and not f.name.startswith('.')]
    
    # Select random artists
    selected_artists = random.sample(artist_folders, min(num_artists, len(artist_folders)))
    
    # Convert to relative paths for discovery
    artist_paths = [str(f.relative_to(dataset_path)) for f in selected_artists]
    
    print("Analyzing artists:")
    for path in artist_paths:
        print(f"- {path}")
    
    # Discover schema
    print("\nDiscovering schema...")
    result = schema.discover_schema(folders=artist_paths)
    
    if result.is_valid:
        print("\nSchema discovery successful!")
        
        # Ensure .blackbird directory exists
        blackbird_dir = dataset_path / ".blackbird"
        blackbird_dir.mkdir(exist_ok=True)
        
        # Save schema to .blackbird/schema.json
        schema_file = blackbird_dir / "schema.json"
        with open(schema_file, 'w', encoding='utf-8') as f:
            json.dump(schema.schema, f, indent=2, ensure_ascii=False)
        
        # Print discovered schema in a more readable format
        print("\nDiscovered Components:")
        for name, config in schema.schema["components"].items():
            print(f"\n{name}:")
            print(f"  Pattern: {config['pattern']}")
            print(f"  Multiple: {config['multiple']}")
            if "description" in config:
                print(f"  Description: {config['description']}")
                
            # Print corresponding statistics
            if name in result.stats:
                stats = result.stats[name]
                print("\n  Statistics:")
                print(f"    Files found: {stats['file_count']}")
                print(f"    Track coverage: {stats['track_coverage']*100:.1f}%")
                print(f"    Unique tracks: {stats['unique_tracks']}")
                print(f"    Files per track: {stats['min_files_per_track']} to {stats['max_files_per_track']}")
                print(f"    Extensions: {', '.join(stats['extensions'])}")
                print(f"    Has sections: {stats['has_sections']}")
        
        print("\nDirectory Structure:")
        print(json.dumps(schema.schema["structure"], indent=2))
        
        print("\nSync Configuration:")
        print(json.dumps(schema.schema["sync"], indent=2))
        
        print(f"\nSchema saved to {schema_file}")
    else:
        print("\nSchema discovery failed with errors:")
        for error in result.errors:
            print(f"- {error}")
        
        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"- {warning}")

def main():
    parser = argparse.ArgumentParser(description='Discover and save schema for a Blackbird dataset')
    parser.add_argument('dataset_path', type=Path, help='Path to the dataset root directory')
    parser.add_argument('--num-artists', type=int, default=25, 
                       help='Number of random artists to analyze (default: 25)')
    
    args = parser.parse_args()
    
    if not args.dataset_path.exists():
        print(f"Error: Dataset path does not exist: {args.dataset_path}")
        return 1
    
    discover_and_save_schema(args.dataset_path, args.num_artists)
    return 0

if __name__ == "__main__":
    exit(main()) 