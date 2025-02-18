#!/usr/bin/env python3

import pytest
from pathlib import Path
import logging
from datetime import datetime
import time
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex

# Set up logging to show only important info
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@pytest.fixture
def real_dataset_path():
    path = Path("/media/k4_nas/disk1/Datasets/Music_RU/Vocal_Dereverb")
    if not path.exists():
        pytest.skip("Real dataset path not found")
    return path

def test_build_real_dataset_index(real_dataset_path):
    """Test building an index from the real dataset.
    This test is separate from the main test suite as it operates on real data."""
    
    # Load schema
    schema_path = real_dataset_path / ".blackbird" / "schema.json"
    if not schema_path.exists():
        pytest.skip("Schema not found in real dataset")
    
    schema = DatasetComponentSchema(real_dataset_path)
    
    # Build index with timing
    logger.info(f"Building index for dataset at {real_dataset_path}")
    start_time = datetime.now()
    
    index = DatasetIndex.build(real_dataset_path, schema)
    
    # Calculate statistics (being mindful of output length)
    duration = (datetime.now() - start_time).total_seconds()
    total_tracks = len(index.tracks)
    total_artists = len(index.album_by_artist)
    total_albums = sum(len(albums) for albums in index.album_by_artist.values())
    total_size_gb = index.total_size / (1024*1024*1024)
    
    # Log summary statistics
    logger.info("\nIndex built successfully!")
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info(f"Total tracks: {total_tracks:,}")
    logger.info(f"Total artists: {total_artists:,}")
    logger.info(f"Total albums: {total_albums:,}")
    logger.info(f"Total size: {total_size_gb:.2f} GB")
    
    # Log sample of data (limited to avoid excessive output)
    logger.info("\nSample of indexed data:")
    
    # Show first 3 artists and their first album
    for artist in list(index.album_by_artist.keys())[:3]:
        albums = index.album_by_artist[artist]
        first_album = next(iter(albums))
        track_count = len(index.track_by_album[first_album])
        logger.info(f"Artist: {artist}")
        logger.info(f"  First album: {Path(first_album).name}")
        logger.info(f"  Tracks in first album: {track_count}")
    
    # Verify index structure
    assert total_tracks > 0, "Index should contain tracks"
    assert total_artists > 0, "Index should contain artists"
    assert total_albums > 0, "Index should contain albums"
    assert index.total_size > 0, "Index should have non-zero total size"
    
    # Verify a few random lookups work
    # Get first track path
    first_track_path = next(iter(index.tracks.keys()))
    track_info = index.tracks[first_track_path]
    
    # Verify track info structure
    assert track_info.artist in index.album_by_artist
    assert track_info.album_path in index.album_by_artist[track_info.artist]
    assert track_info.track_path in index.track_by_album[track_info.album_path]
    assert len(track_info.files) > 0
    assert all(isinstance(p, str) for p in track_info.files.values())
    assert all(isinstance(s, int) for s in track_info.file_sizes.values()) 