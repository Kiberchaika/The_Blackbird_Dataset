from tqdm import tqdm
from pathlib import Path
import json
from collections import defaultdict
from typing import Dict, List, Set

def analyze_dataset_structure(root_path: str) -> Dict:
    root = Path(root_path)
    structure = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    data_types = set()  # Now a global set
    
    artists = list(root.iterdir())
    artist_progress = tqdm(artists, desc="Processing artists")
    
    for artist_dir in artist_progress:
        if not artist_dir.is_dir():
            continue
            
        albums = list(artist_dir.iterdir())
        album_progress = tqdm(albums, desc=f"Albums for {artist_dir.name}", leave=False)
        
        for album_dir in album_progress:
            if not album_dir.is_dir():
                continue
                
            current_path = album_dir
            cd_dirs = [d for d in album_dir.iterdir() if d.is_dir() and d.name.startswith('CD')]
            
            if cd_dirs:
                for cd_dir in cd_dirs:
                    analyze_tracks(cd_dir, artist_dir.name, album_dir.name, 
                                 cd_dir.name, structure, data_types)
            else:
                analyze_tracks(current_path, artist_dir.name, album_dir.name, 
                             None, structure, data_types)

    return {
        'dataset_structure': structure,
        'data_types': list(data_types)  # Convert set to list for JSON
    }

def analyze_tracks(directory: Path, artist: str, album: str, 
                  cd: str | None, structure: Dict, data_types: Set[str]):
    for track in directory.glob('*.*'):
        if not track.is_file():
            continue
            
        stem = track.stem
        suffix = track.suffix
        
        parts = stem.split('_')
        postfix = f"_{parts[-1]}" if len(parts) > 1 else ""
        
        if cd:
            structure[artist][album][cd].add(track.name)
        else:
            structure[artist][album]["tracks"].add(track.name)
            
        data_type = f"{postfix}{suffix}"
        if data_type == suffix:
            data_type = "source" + suffix
            
        if data_type not in data_types:
            print(f"\nFound new data type: {data_type}")
        data_types.add(data_type)

def save_analysis(analysis: Dict, output_file: str = 'dataset_analysis.json'):
    # Convert all nested sets to lists for JSON serialization
    def convert_sets(obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, dict):
            return {k: convert_sets(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [convert_sets(item) for item in obj]  # Fixed: was using x instead of item
        return obj
    
    analysis = convert_sets(analysis)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    dataset_path = "/media/k4_nas/Datasets/Music_RU/Separated"
    analysis = analyze_dataset_structure(dataset_path)
    save_analysis(analysis)