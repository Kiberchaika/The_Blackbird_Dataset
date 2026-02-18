#!/usr/bin/env python3
"""
Debug script to test WebDAV downloads with special characters.
This script tests downloading specific files with # symbols from a real WebDAV server.
"""

import sys
import os
from pathlib import Path
import logging
import shutil
from blackbird.sync import WebDAVClient

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_special_char_downloads(webdav_url="webdav://localhost:7771"):
    """Test downloading files with special characters from WebDAV server."""
    
    # Create test directory
    test_dir = Path("test_special_chars_debug")
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True)
    
    # List of problematic files from the error logs
    problem_files = [
        "АнимациЯ/Распутье [2015]/05.АнимациЯ - #непорусски.mir.json",
        "Градусы/Градус 100 [2016]/01.Градусы - #Валигуляй.mir.json",
        "Александр Ливер/Проффессионнал [2003]/06.Александр Ливер - Школьная история #1.mir.json",
        "Александр Ливер/Проффессионнал [2003]/08.Александр Ливер - Школьная история #2.mir.json",
        "E-SEX-T/Время Слона [2000]/02.E-SEX-T - Всё сложней (Облом #2).mir.json"
    ]
    
    # Create WebDAV client
    client = WebDAVClient(webdav_url)
    
    # Test each file
    success_count = 0
    failure_count = 0
    
    for file_path in problem_files:
        logger.info(f"Testing download of: {file_path}")
        local_file = test_dir / file_path
        
        # Create parent directory
        local_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Try to download the file
        try:
            success = client.download_file(file_path, local_file)
            
            if success and local_file.exists():
                logger.info(f"✅ SUCCESS: Downloaded {file_path}")
                logger.info(f"  File size: {local_file.stat().st_size} bytes")
                success_count += 1
            else:
                logger.error(f"❌ FAILED: Could not download {file_path}")
                failure_count += 1
        except Exception as e:
            logger.error(f"❌ ERROR: Exception while downloading {file_path}: {e}")
            failure_count += 1
    
    # Print summary
    logger.info("\n" + "="*50)
    logger.info(f"DOWNLOAD TEST SUMMARY")
    logger.info("="*50)
    logger.info(f"Total files tested: {len(problem_files)}")
    logger.info(f"Successful downloads: {success_count}")
    logger.info(f"Failed downloads: {failure_count}")
    
    # Clean up
    if test_dir.exists():
        shutil.rmtree(test_dir)
    
    return success_count, failure_count

if __name__ == "__main__":
    # Get WebDAV URL from command line if provided
    webdav_url = sys.argv[1] if len(sys.argv) > 1 else "webdav://localhost:7771"
    
    logger.info(f"Testing WebDAV downloads with special characters from {webdav_url}")
    success, failure = test_special_char_downloads(webdav_url)
    
    # Exit with appropriate status code
    sys.exit(1 if failure > 0 else 0) 