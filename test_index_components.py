#!/usr/bin/env python3

from pathlib import Path
from blackbird.index import DatasetIndex
from blackbird.schema import DatasetComponentSchema
from collections import defaultdict

def main():
    # Path to the dataset
    dataset_path = Path("/media/k4_nas/disk1/Datasets/Music_Part1")
    
    # Load schema and index
    schema = DatasetComponentSchema.load(dataset_path / ".blackbird" / "schema.json")
    index = DatasetIndex.load(dataset_path / ".blackbird" / "index.pickle")
    
    # Analyze index contents
    component_counts = defaultdict(int)
    component_sizes = defaultdict(int)
    
    for track_info in index.tracks.values():
        for comp_name, file_path in track_info.files.items():
            component_counts[comp_name] += 1
            component_sizes[comp_name] += track_info.file_sizes[file_path]
    
    # Print summary
    print(f"\nDataset Summary:")
    print(f"Total tracks: {len(index.tracks)}")
    print(f"Total artists: {len(index.album_by_artist)}")
    
    print("\nComponent Analysis:")
    print("------------------")
    for comp_name, count in sorted(component_counts.items()):
        size_gb = component_sizes[comp_name] / (1024*1024*1024)
        pattern = schema.schema["components"].get(comp_name, {}).get("pattern", "unknown pattern")
        print(f"\n{comp_name}:")
        print(f"  Pattern: {pattern}")
        print(f"  Files found: {count}")
        print(f"  Total size: {size_gb:.2f} GB")

if __name__ == "__main__":
    main() 