import os
import logging
import itertools
import shutil
from pathlib import Path
from blackbird.sync import DatasetSync, SyncState

# Enable debug logging for blackbird module
logging.getLogger('blackbird').setLevel(logging.DEBUG)
logging.getLogger('webdav3').setLevel(logging.INFO)

# Test artist to use
TEST_ARTIST = "19_84"

# Local test directory where we'll sync to
LOCAL_TEST_DIR = "test_sync_data"

def list_directory_contents(path):
    """List contents of a directory, showing only first 5 items."""
    items = []
    for root, dirs, files in os.walk(path):
        level = root.replace(path, '').count(os.sep)
        indent = ' ' * 4 * level
        items.append(f"{indent}{os.path.basename(root)}/")
        if files:
            subindent = ' ' * 4 * (level + 1)
            for f in sorted(files)[:5]:  # Only show first 5 files
                items.append(f"{subindent}{f}")
    return items

def main():
    # Clean up any existing test data
    print("\nCleaning up existing test data...")
    if os.path.exists(LOCAL_TEST_DIR):
        print(f"Removing existing test directory: {LOCAL_TEST_DIR}")
        shutil.rmtree(LOCAL_TEST_DIR)
    
    # Create fresh test directory
    print("Creating fresh test directory...")
    os.makedirs(LOCAL_TEST_DIR)
    
    # Create sync manager
    sync = DatasetSync(Path(LOCAL_TEST_DIR))
    
    # Configure WebDAV client
    client = sync.configure_client(
        webdav_url="http://localhost:8080",
        username="user",
        password="test123"
    )
    
    # First sync instrumental files
    print("\nSyncing instrumental files...")
    sync.sync(
        client,
        components=['instrumental_audio'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=False  # Don't resume for first sync
    )
    
    # Then sync vocal files
    print("\nSyncing vocal files...")
    sync.sync(
        client,
        components=['vocals_audio'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=True  # Resume to preserve instrumental files
    )

    # Then sync mir files
    print("\nSyncing mir files...")
    sync.sync(
        client,
        components=['mir'],
        artists=[TEST_ARTIST],  # Only sync test artist
        resume=True  # Resume to preserve instrumental files
    )
    
    # Print final directory structure
    print("\nFinal directory structure:")
    for item in list_directory_contents(os.path.join(LOCAL_TEST_DIR, TEST_ARTIST)):
        print(item)

if __name__ == "__main__":
    main() 