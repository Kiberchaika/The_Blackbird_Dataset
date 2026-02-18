#!/usr/bin/env python3
"""
Test script to demonstrate optimized sync functionality.
This script compares the performance of different sync configurations.
"""

import time
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import shutil
import os

from blackbird.sync import configure_client, clone_dataset, ProfilingStats

def run_test(
    source_url: str,
    destination: str,
    components: list,
    artists: list,
    parallel: int = 1,
    use_http2: bool = False,
    connection_pool_size: int = 10,
    test_name: str = "Test"
):
    """Run a sync test with the given parameters."""
    # Create a clean destination directory
    dest_path = Path(destination) / f"test_{test_name.replace(' ', '_').lower()}"
    if dest_path.exists():
        shutil.rmtree(dest_path)
    dest_path.mkdir(parents=True, exist_ok=True)
    
    # Run the sync with profiling
    print(f"\nRunning test: {test_name}")
    print(f"  Parallel downloads: {parallel}")
    print(f"  HTTP/2: {use_http2}")
    print(f"  Connection pool size: {connection_pool_size}")
    
    start_time = time.time()
    stats = clone_dataset(
        source_url=source_url,
        destination=dest_path,
        components=components,
        artists=artists,
        enable_profiling=True,
        parallel=parallel,
        use_http2=use_http2,
        connection_pool_size=connection_pool_size
    )
    end_time = time.time()
    
    # Print summary
    total_time = end_time - start_time
    print(f"\nTest Results for {test_name}:")
    print(f"  Total time: {total_time:.2f} seconds")
    print(f"  Total files: {stats.total_files}")
    print(f"  Downloaded: {stats.downloaded_files}")
    print(f"  Failed: {stats.failed_files}")
    print(f"  Skipped: {stats.skipped_files}")
    print(f"  Total size: {stats.total_size / (1024*1024*1024):.2f} GB")
    print(f"  Downloaded size: {stats.downloaded_size / (1024*1024*1024):.2f} GB")
    print(f"  Download speed: {stats.downloaded_size / (1024*1024) / total_time:.2f} MB/s")
    
    # Print profiling stats
    if stats.profiling:
        print("\nProfiling Statistics:")
        profile_summary = stats.profiling.get_summary()
        
        # Sort operations by percentage of total time
        sorted_ops = sorted(profile_summary.items(), key=lambda x: x[1]['percentage'], reverse=True)
        
        for op, metrics in sorted_ops[:10]:  # Show top 10 operations
            print(f"  {op}:")
            print(f"    Total: {metrics['total_ms']:.2f} ms")
            print(f"    Calls: {metrics['calls']}")
            print(f"    Avg: {metrics['avg_ms']:.2f} ms per call")
            print(f"    Percentage: {metrics['percentage']:.2f}%")
    
    return {
        "name": test_name,
        "total_time": total_time,
        "downloaded_files": stats.downloaded_files,
        "downloaded_size": stats.downloaded_size,
        "download_speed": stats.downloaded_size / (1024*1024) / total_time if total_time > 0 else 0,
        "profiling": stats.profiling.get_summary() if stats.profiling else {}
    }

def plot_results(results):
    """Plot the results of the tests."""
    # Create figure with multiple subplots
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    
    # Extract data for plotting
    names = [r["name"] for r in results]
    times = [r["total_time"] for r in results]
    speeds = [r["download_speed"] for r in results]
    
    # Plot total time
    axs[0, 0].bar(names, times)
    axs[0, 0].set_title('Total Time (seconds)')
    axs[0, 0].set_ylabel('Seconds')
    axs[0, 0].tick_params(axis='x', rotation=45)
    
    # Plot download speed
    axs[0, 1].bar(names, speeds)
    axs[0, 1].set_title('Download Speed (MB/s)')
    axs[0, 1].set_ylabel('MB/s')
    axs[0, 1].tick_params(axis='x', rotation=45)
    
    # Plot profiling data for each test
    for i, result in enumerate(results):
        if "profiling" in result and result["profiling"]:
            # Get top 5 operations by percentage
            sorted_ops = sorted(result["profiling"].items(), key=lambda x: x[1]['percentage'], reverse=True)[:5]
            op_names = [op for op, _ in sorted_ops]
            op_percentages = [metrics['percentage'] for _, metrics in sorted_ops]
            
            # Plot as pie chart
            axs[1, i % 2].pie(op_percentages, labels=op_names, autopct='%1.1f%%')
            axs[1, i % 2].set_title(f'Time Breakdown: {result["name"]}')
    
    plt.tight_layout()
    plt.savefig('sync_performance_comparison.png')
    print("\nPerformance comparison chart saved to 'sync_performance_comparison.png'")

def main():
    parser = argparse.ArgumentParser(description='Test optimized sync functionality')
    parser.add_argument('source', help='WebDAV URL of the source dataset')
    parser.add_argument('--destination', default='./test_results', help='Destination directory for test results')
    parser.add_argument('--components', default='vocals,accompaniment', help='Comma-separated list of components to sync')
    parser.add_argument('--artists', default='The Beatles', help='Comma-separated list of artists to sync')
    parser.add_argument('--skip-plots', action='store_true', help='Skip generating performance plots')
    args = parser.parse_args()
    
    # Parse components and artists
    components = args.components.split(',')
    artists = args.artists.split(',')
    
    # Create destination directory
    dest_path = Path(args.destination)
    dest_path.mkdir(parents=True, exist_ok=True)
    
    # Run tests with different configurations
    results = []
    
    # Test 1: Baseline (sequential, no optimizations)
    results.append(run_test(
        source_url=args.source,
        destination=args.destination,
        components=components,
        artists=artists,
        parallel=1,
        use_http2=False,
        connection_pool_size=0,
        test_name="Baseline"
    ))
    
    # Test 2: Connection pooling only
    results.append(run_test(
        source_url=args.source,
        destination=args.destination,
        components=components,
        artists=artists,
        parallel=1,
        use_http2=False,
        connection_pool_size=10,
        test_name="Connection Pooling"
    ))
    
    # Test 3: Parallel downloads only
    results.append(run_test(
        source_url=args.source,
        destination=args.destination,
        components=components,
        artists=artists,
        parallel=4,
        use_http2=False,
        connection_pool_size=0,
        test_name="Parallel (4 threads)"
    ))
    
    # Test 4: HTTP/2 only (if available)
    try:
        import httpx
        results.append(run_test(
            source_url=args.source,
            destination=args.destination,
            components=components,
            artists=artists,
            parallel=1,
            use_http2=True,
            connection_pool_size=0,
            test_name="HTTP/2"
        ))
    except ImportError:
        print("\nSkipping HTTP/2 test as httpx is not installed")
    
    # Test 5: All optimizations
    results.append(run_test(
        source_url=args.source,
        destination=args.destination,
        components=components,
        artists=artists,
        parallel=4,
        use_http2=True,
        connection_pool_size=10,
        test_name="All Optimizations"
    ))
    
    # Print comparison summary
    print("\nPerformance Comparison:")
    baseline_time = results[0]["total_time"]
    for result in results:
        speedup = baseline_time / result["total_time"] if result["total_time"] > 0 else 0
        print(f"  {result['name']}:")
        print(f"    Total time: {result['total_time']:.2f} seconds")
        print(f"    Download speed: {result['download_speed']:.2f} MB/s")
        print(f"    Speedup vs baseline: {speedup:.2f}x")
    
    # Plot results if matplotlib is available and not skipped
    if not args.skip_plots:
        try:
            plot_results(results)
        except Exception as e:
            print(f"Failed to generate plots: {e}")
            print("Make sure matplotlib is installed: pip install matplotlib")

if __name__ == "__main__":
    main() 