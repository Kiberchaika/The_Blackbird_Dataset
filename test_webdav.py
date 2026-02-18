#!/usr/bin/env python3
"""
Debug script to test the WebDAV client directly.
"""

import logging
import sys
import traceback
from pathlib import Path
from blackbird.sync import WebDAVClient, configure_client

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))

def test_webdav_client():
    """Test the WebDAV client directly."""
    print("Testing WebDAV client...")
    
    # Test with direct initialization
    try:
        print("\nTesting direct initialization...")
        client = WebDAVClient("webdav://localhost:7771")
        print(f"Client initialized: {client}")
        
        # Try to list files
        print("\nTrying to list files...")
        try:
            files = client.client.list()  # Use the underlying client
            print(f"Files: {files[:5] if files else 'No files found'}")
        except Exception as e:
            print(f"Error listing files: {e}")
            traceback.print_exc()
        
        # Try to download schema
        print("\nTrying to download schema...")
        dest_path = Path("./test_schema.json")
        try:
            # Debug the get_full_path method
            print(f"Remote path: .blackbird/schema.json")
            try:
                full_path = client.client.get_full_path(".blackbird/schema.json")
                print(f"Full path: {full_path}")
            except Exception as e:
                print(f"Error getting full path: {e}")
                traceback.print_exc()
            
            # Try direct download with the underlying client
            print("\nTrying direct download with underlying client...")
            try:
                client.client.download_sync(
                    remote_path=".blackbird/schema.json",
                    local_path=str(dest_path)
                )
                print("Direct download successful")
                if dest_path.exists():
                    print(f"File size: {dest_path.stat().st_size} bytes")
            except Exception as e:
                print(f"Error with direct download: {e}")
                traceback.print_exc()
            
            # Try with our wrapper method
            print("\nTrying with our wrapper method...")
            result = client.download_file(".blackbird/schema.json", dest_path)
            print(f"Download result: {result}")
            if dest_path.exists():
                print(f"File size: {dest_path.stat().st_size} bytes")
        except Exception as e:
            print(f"Error downloading schema: {e}")
            traceback.print_exc()
    
    except Exception as e:
        print(f"Error initializing client: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_webdav_client() 