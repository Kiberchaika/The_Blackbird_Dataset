#!/usr/bin/env python3

import sys
from pathlib import Path
import logging
from datetime import datetime
import time

# Add parent directory to path to import blackbird
sys.path.append(str(Path(__file__).parent.parent))
from blackbird.schema import DatasetComponentSchema
from blackbird.index import build_index

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indexing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Dataset path - adjust as needed
    dataset_path = Path("/media/k4_nas/Datasets/Music_RU/Vocal_Dereverb")
    if not dataset_path.exists():
        logger.error(f"Dataset path not found: {dataset_path}")
        sys.exit(1)

    # Load schema from existing file
    schema_path = dataset_path / ".blackbird" / "schema.json"
    if not schema_path.exists():
        logger.error(f"Schema not found at {schema_path}")
        sys.exit(1)
    schema = DatasetComponentSchema.load(schema_path)

    # Build index with timing information
    logger.info(f"Building index for dataset at {dataset_path}")
    start_time = datetime.now()
    
    try:
        index = build_index(dataset_path, schema)
        
        # Save index
        t_save_start = time.time()
        index_path = schema_path.parent / "index.pickle"
        index.save(index_path)
        t_save = (time.time() - t_save_start) * 1000
        
        # Log statistics
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\nIndex built successfully!")
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Save time: {t_save:.0f}ms")
        logger.info(f"Total tracks: {len(index.tracks)}")
        logger.info(f"Total artists: {len(index.album_by_artist)}")
        logger.info(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
        logger.info(f"Total size: {index.total_size / (1024*1024*1024):.2f} GB")
        logger.info(f"Index saved to: {index_path}")
        
    except Exception as e:
        logger.error(f"Error building index: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 