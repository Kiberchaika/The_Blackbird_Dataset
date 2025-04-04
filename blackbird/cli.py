import click
from pathlib import Path
from typing import List, Optional
from .dataset import Dataset
from .schema import DatasetComponentSchema
from .sync import clone_dataset, SyncStats, configure_client, ProfilingStats, DatasetSync, resume_sync_operation
from .index import DatasetIndex
import json
import sys
import tempfile
import shutil
from collections import defaultdict
import random
from tqdm import tqdm
import time
import logging
import os
from .locations import LocationsManager, LocationValidationError, SymbolicPathError, resolve_symbolic_path
from .utils import format_size # Assuming format_size exists in utils
from colorama import Fore
from .operations import load_operation_state, delete_operation_state, find_latest_state_file, OperationState
from .mover import move_data  # Import move_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import colorama for cross-platform colored terminal output
try:
    from colorama import init, Fore, Back, Style
    init(autoreset=True)  # Initialize colorama with autoreset
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Create dummy color classes if colorama is not available
    class DummyColor:
        def __getattr__(self, name):
            return ""
    Fore = DummyColor()
    Back = DummyColor()
    Style = DummyColor() # Define Style even if colorama is missing

def _is_dataset_dir(path: Path) -> bool:
    """Check if a directory appears to be a Blackbird dataset root."""
    return (path / ".blackbird").is_dir()

@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx):
    """Blackbird Dataset Manager CLI"""
    if ctx.invoked_subcommand is None:
        # Check if CWD is a dataset directory
        cwd = Path.cwd()
        if _is_dataset_dir(cwd):
            click.echo(f"Blackbird Dataset Status ({cwd}):")
            try:
                # Initialize managers
                locations_manager = LocationsManager(cwd)
                locations_manager.load_locations()
                dataset = Dataset(cwd) # Initializes locations via LocationsManager
                
                # Print Locations
                click.echo("\nLocations:")
                locs = locations_manager.get_all_locations()
                if not locs:
                    click.echo("  No locations configured (using default 'Main').")
                else:
                    for name, path in locs.items():
                        click.echo(f"  - {name}: {path}")

                # Print Index Info
                click.echo("\nIndex:")
                try:
                    index_path = dataset.path / ".blackbird" / "index.pickle"
                    if not index_path.exists():
                       raise FileNotFoundError("Index file not found")
                    index = DatasetIndex.load(index_path) # Correct way to load
                    click.echo(f"  Last updated: {index.last_updated}")
                    click.echo("  Statistics by Location:")
                    if not index.stats_by_location:
                         click.echo("    No location-specific stats found (re-index needed?).")
                    else:
                        for loc_name, stats in index.stats_by_location.items():
                            click.echo(f"    {loc_name}:")
                            click.echo(f"      Files: {stats.get('file_count', 0)}")
                            click.echo(f"      Size: {format_size(stats.get('total_size', 0))}")
                            click.echo(f"      Tracks: {stats.get('track_count', 0)}")
                            click.echo(f"      Albums: {stats.get('album_count', 0)}")
                            click.echo(f"      Artists: {stats.get('artist_count', 0)}")
                except FileNotFoundError:
                    click.echo("  Index file (.blackbird/index.pickle) not found. Run 'blackbird reindex .'")
                except Exception as e:
                    click.echo(f"  Error loading index: {e}", err=True)
                    
            except LocationValidationError as e:
                 click.echo(f"Error loading locations: {e}", err=True)
            except Exception as e:
                click.echo(f"An unexpected error occurred: {e}", err=True)
        else:
             # Not a dataset dir, show help (default Click behavior)
             click.echo(ctx.get_help())

@main.command()
@click.argument('source')
@click.argument('destination')
@click.option('--components', help='Comma-separated list of components to clone')
@click.option('--missing', help='Only clone components for tracks missing this component')
@click.option('--artists', help='Comma-separated list of artists to clone (supports glob patterns)')
@click.option('--proportion', type=float, help='Proportion of dataset to clone (0-1)')
@click.option('--offset', type=int, default=0, help='Offset for proportion-based cloning')
@click.option('--profile', is_flag=True, help='Enable performance profiling')
@click.option('--parallel', type=int, default=1, help='Number of parallel downloads (1 for sequential)')
@click.option('--http2', is_flag=True, help='Use HTTP/2 for connections if available')
@click.option('--connection-pool', type=int, default=10, help='Size of the connection pool')
@click.option('--target-location', default='Main', help='Name of the location to clone files into')
def clone(source: str, destination: str, components: Optional[str], missing: Optional[str],
         artists: Optional[str], proportion: Optional[float], offset: Optional[int], profile: bool,
         parallel: int, http2: bool, connection_pool: int, target_location: str):
    """Clone dataset from remote source.
    
    SOURCE: Remote dataset URL (e.g. webdav://server/dataset)
    DESTINATION: Local path for the cloned dataset
    """
    try:
        # Convert comma-separated strings to lists
        component_list = components.split(',') if components else None
        artist_list = artists.split(',') if artists else None
        
        # --- Use Path object consistently --- 
        dest_path = Path(destination)
        
        # Validate proportion
        if proportion is not None and not (0 < proportion <= 1):
            raise ValueError("Proportion must be between 0 and 1")
            
        click.echo(f"Cloning from {source} to {dest_path}")
        if component_list:
            click.echo(f"Components: {', '.join(component_list)}")
        if missing:
            click.echo(f"Only for tracks missing: {missing}")
        if artist_list:
            click.echo(f"Artists: {', '.join(artist_list)}")
        if proportion:
            click.echo(f"Proportion: {proportion} (offset: {offset})")
            
        # Clone dataset
        stats = clone_dataset(
            source_url=source,
            destination=dest_path,
            components=component_list,
            missing_component=missing,
            artists=artist_list,
            proportion=proportion,
            offset=offset,
            enable_profiling=profile,
            parallel=parallel,
            use_http2=http2,
            connection_pool_size=connection_pool,
            target_location=target_location
        )
        
        # Use the LocationsManager to ensure default locations are handled
        locations_manager = LocationsManager(dest_path)
        try:
            # Load locations (creates default 'Main' in memory if file missing)
            locations_manager.load_locations()
            
            # Determine the path for the target location (or default)
            target_path_str = locations_manager.get_location_path(target_location)
            
            # Ensure the directory for the target location exists
            Path(target_path_str).mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists for location '{target_location}': {target_path_str}")

            # Save locations only if the file didn't exist initially
            if not locations_manager.locations_file_path.exists():
                click.echo("Saving default locations file...")
                locations_manager.save_locations()
                click.echo(f"Successfully created locations file at {locations_manager.locations_file_path}")

        except LocationValidationError as e:
             logger.error(f"Error handling locations after clone: {e}", exc_info=True)
             click.echo(f"{Fore.RED}Warning: Error setting up locations after clone: {e}")
        except Exception as e:
             logger.error(f"Unexpected error setting up locations after clone: {e}", exc_info=True)
             click.echo(f"{Fore.RED}Warning: Unexpected error setting up locations after clone.")
             # Don't prevent clone completion message for this

        click.echo(f"\n{Fore.GREEN}Clone completed!")
        click.echo(f"Total files: {stats.total_files}")
        click.echo(f"Downloaded: {stats.downloaded_files}")
        click.echo(f"Failed: {stats.failed_files}")
        click.echo(f"Total size: {stats.total_size / (1024*1024*1024):.2f} GB")
        click.echo(f"Downloaded size: {stats.downloaded_size / (1024*1024*1024):.2f} GB")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.command()
@click.argument('source')
@click.argument('destination')
@click.option('--components', help='Comma-separated list of components to sync')
@click.option('--missing', help='Only sync components for tracks missing this component')
@click.option('--artists', help='Comma-separated list of artists to sync (supports glob patterns)')
@click.option('--albums', help='Comma-separated list of albums to sync (requires --artists to be specified)')
@click.option('--profile', is_flag=True, help='Enable performance profiling')
@click.option('--parallel', type=int, default=1, help='Number of parallel downloads (1 for sequential)')
@click.option('--http2', is_flag=True, help='Use HTTP/2 for connections if available')
@click.option('--connection-pool', type=int, default=10, help='Size of the connection pool')
@click.option('--force-reindex', is_flag=True, help='Force reindex of local dataset before syncing')
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--target-location', default='Main', help='Name of the location to sync files into')
def sync(source: str, destination: str, components: Optional[str], missing: Optional[str],
         artists: Optional[str], albums: Optional[str], profile: bool, parallel: int, http2: bool, 
         connection_pool: int, force_reindex: bool, debug: bool, target_location: str):
    """Sync dataset from remote source to local dataset.
    
    SOURCE: Remote dataset URL (webdav://[user:pass@]host[:port]/path)
    DESTINATION: Local path to dataset
    """
    try:
        # Configure logging
        if debug:
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('blackbird').setLevel(logging.DEBUG)
            click.echo("Debug logging enabled")
        
        # Parse components
        component_list = components.split(',') if components else None
        
        # Parse artists
        artist_list = artists.split(',') if artists else None
        
        # Parse albums
        album_list = albums.split(',') if albums else None
        
        # Check if destination exists
        dest_path = Path(destination)
        if not dest_path.exists():
            click.echo(f"Destination path '{destination}' does not exist. Creating...")
            dest_path.mkdir(parents=True, exist_ok=True)
        
        # Check if destination is a dataset
        blackbird_dir = dest_path / ".blackbird"
        schema_path = blackbird_dir / "schema.json"
        index_path = blackbird_dir / "index.pickle"
        
        # Configure WebDAV client
        client = configure_client(source, use_http2=http2, connection_pool_size=connection_pool)
        
        # Check if we need to set up a new dataset
        if not blackbird_dir.exists() or not schema_path.exists() or not index_path.exists():
            click.echo("Destination is not a dataset. Setting up new dataset...")
            
            # Create blackbird directory
            blackbird_dir.mkdir(exist_ok=True)
            
            # Download schema
            click.echo("Downloading schema...")
            if not client.download_file(".blackbird/schema.json", schema_path):
                raise ValueError(f"Failed to download schema from {source}")
            
            # Download index
            click.echo("Downloading index...")
            if not client.download_file(".blackbird/index.pickle", index_path):
                raise ValueError(f"Failed to download index from {source}")
            
            # Create Dataset object (will load downloaded schema/index)
            dataset = Dataset(dest_path)
            
            # Create dataset sync using the Dataset object
            dataset_sync = DatasetSync(dataset)
            
        else:
            # Load existing dataset
            click.echo("Loading existing dataset...")
            dataset = Dataset(dest_path) # Create Dataset object first
            
            # Force reindex if requested
            if force_reindex:
                click.echo("Forcing reindex of local dataset...")
                dataset.rebuild_index()
                click.echo("Reindex complete.")
                # Need to reload the Dataset object to get the new index
                dataset = Dataset(dest_path) 
            
            # Create dataset sync using the Dataset object
            dataset_sync = DatasetSync(dataset)
            
            # Verify that we have the same components
            remote_schema_path = Path(tempfile.mkdtemp()) / "schema.json"
            if not client.download_file(".blackbird/schema.json", remote_schema_path):
                raise ValueError(f"Failed to download schema from {source}")
            
            remote_schema = DatasetComponentSchema.load(remote_schema_path)
            local_schema = dataset_sync.schema
            
            # Check if components match
            remote_components = set(remote_schema.schema.get('components', {}).keys())
            local_components = set(local_schema.schema.get('components', {}).keys())
            
            if remote_components != local_components:
                click.echo("Warning: Remote and local component schemas don't match.")
                click.echo(f"Remote components: {sorted(remote_components)}")
                click.echo(f"Local components: {sorted(local_components)}")
                if not click.confirm("Continue anyway?"):
                    click.echo("Sync aborted.")
                    return
            
        # Now perform the sync using DatasetSync
        click.echo("Starting sync operation...")
        if force_reindex:
            click.echo("Note: Local dataset was reindexed, which should detect existing files correctly.")
        
        # Ensure schema has components
        if not dataset_sync.schema.schema.get('components'):
            click.echo("Warning: Local schema has no components defined. Copying components from remote schema.")
            dataset.schema.schema['components'] = remote_schema.schema['components']
            dataset.schema.save()
            click.echo("Schema updated with components from remote.")
            
            # Rebuild index with updated schema
            click.echo("Rebuilding index with updated schema...")
            dataset.rebuild_index()
            
            # Reload DatasetSync with updated index - Recreate Dataset object first
            dataset = Dataset(dest_path) # Re-init Dataset to pick up new index
            dataset_sync = DatasetSync(dataset)
        
        sync_stats = dataset_sync.sync(
            client=client,
            components=component_list if component_list else list(dataset_sync.schema.schema.get('components', {}).keys()),
            artists=artist_list,
            albums=album_list,
            missing_component=missing,
            resume=True,
            enable_profiling=profile,
            parallel=parallel,
            use_http2=http2,
            connection_pool_size=connection_pool,
            target_location_name=target_location
        )
        
        click.echo(f"\n{Fore.GREEN}Sync completed!")
        click.echo(f"Total files: {sync_stats.total_files}")
        click.echo(f"Downloaded: {sync_stats.downloaded_files}")
        click.echo(f"Failed: {sync_stats.failed_files}")
        click.echo(f"Skipped: {sync_stats.skipped_files}")
        click.echo(f"Total size: {sync_stats.total_size / (1024*1024*1024):.2f} GB")
        click.echo(f"Downloaded size: {sync_stats.downloaded_size / (1024*1024*1024):.2f} GB")
        
        # Print profiling stats if enabled
        if profile and sync_stats.profiling:
            click.echo("\nProfiling Statistics:")
            summary = sync_stats.profiling.get_summary()
            
            # Sort operations by percentage of total time
            sorted_ops = sorted(summary.items(), key=lambda x: x[1]['percentage'], reverse=True)
            
            for op, metrics in sorted_ops:
                click.echo(f"  {op}:")
                click.echo(f"    Total: {metrics['total_ms']:.2f} ms")
                click.echo(f"    Calls: {metrics['calls']}")
                click.echo(f"    Avg: {metrics['avg_ms']:.2f} ms per call")
                click.echo(f"    Percentage: {metrics['percentage']:.2f}%")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.command()
@click.argument('state_file_path', type=click.Path(dir_okay=False, resolve_path=True))
@click.option('--dataset-path', type=click.Path(exists=True, file_okay=False, resolve_path=True),
              help='Path to the dataset directory (defaults to CWD if state file is inside .blackbird)')
@click.option('--profile', is_flag=True, help='Enable performance profiling')
@click.option('--parallel', type=int, default=1, help='Number of parallel downloads (1 for sequential)')
@click.option('--http2', is_flag=True, help='Use HTTP/2 for connections if available')
@click.option('--connection-pool', type=int, default=10, help='Size of the connection pool')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def resume(state_file_path: str, dataset_path: Optional[str], profile: bool, parallel: int, http2: bool,
           connection_pool: int, debug: bool):
    """Resume an interrupted sync or move operation from a state file."""
    try:
        # Configure logging
        if debug:
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('blackbird').setLevel(logging.DEBUG)
            click.echo("Debug logging enabled")

        state_path = Path(state_file_path)

        # Determine dataset path
        if dataset_path:
            ds_path = Path(dataset_path)
        else:
            # Try to infer from state file location
            if ".blackbird" in state_path.parts:
                blackbird_index = state_path.parts.index(".blackbird")
                ds_path = Path(*state_path.parts[:blackbird_index])
                click.echo(f"Inferred dataset path from state file: {ds_path}")
            else:
                # Fallback to CWD, but warn user
                ds_path = Path.cwd()
                click.echo(f"{Fore.YELLOW}Warning: Could not infer dataset path from state file location. Assuming current directory: {ds_path}{Style.RESET_ALL}")
                if not (ds_path / ".blackbird").is_dir():
                     raise click.UsageError(f"Current directory {ds_path} does not appear to be a dataset. Please specify --dataset-path.")

        if not (ds_path / ".blackbird").is_dir():
             raise click.UsageError(f"Specified or inferred path {ds_path} is not a valid dataset directory.")

        # Load operation state
        state = load_operation_state(state_path)
        if not state:
            raise click.ClickException(f"Failed to load or parse state file: {state_path}")

        click.echo(f"Resuming {state['operation_type']} operation for target location '{state['target_location']}' using state file: {state_path}")

        # --- Resume Logic (call appropriate function) ---
        if state['operation_type'] == 'sync':
            success = resume_sync_operation(
                dataset_path=ds_path,
                state_file_path=state_path,
                state=state,
                enable_profiling=profile,
                parallel=parallel,
                use_http2=http2,
                connection_pool_size=connection_pool
            )
        elif state['operation_type'] == 'move':
            click.echo(f"{Fore.YELLOW}Resuming 'move' operations is not yet implemented.{Style.RESET_ALL}")
            # success = resume_move_operation(...) # Placeholder
            success = False # Mark as not successful for now
        else:
            raise click.ClickException(f"Unknown operation type in state file: {state['operation_type']}")

        if success:
            click.echo(f"{Fore.GREEN}Resume operation completed successfully.{Style.RESET_ALL}")
            # State file should be deleted by the resume function on success
        else:
            click.echo(f"{Fore.RED}Resume operation finished with errors or was aborted. State file kept: {state_path}{Style.RESET_ALL}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Resume operation failed: {e}", exc_info=debug)
        click.echo(f"{Fore.RED}Error during resume: {str(e)}{Style.RESET_ALL}", err=True)
        sys.exit(1)

@main.command()
@click.argument('dataset_path')
@click.option('--missing', help='Show statistics for tracks missing this component')
def stats(dataset_path: str, missing: Optional[str]):
    """Show dataset statistics.
    
    DATASET_PATH: Path to the dataset or WebDAV URL
    """
    try:
        # Check if it's a WebDAV URL
        if dataset_path.startswith(('http://', 'https://', 'webdav://')):
            # Create a fixed temporary directory for downloading index
            temp_dir = '/tmp/blackbird_stats_temp'
            temp_path = Path(temp_dir)
            temp_path.mkdir(parents=True, exist_ok=True)
            
            # Configure WebDAV client
            client = configure_client(dataset_path)
            
            # Download index
            click.echo("Downloading index from remote...")
            index_path = temp_path / '.blackbird' / 'index.pickle'
            index_path.parent.mkdir(parents=True, exist_ok=True)
            
            if not client.download_file('.blackbird/index.pickle', index_path):
                raise ValueError("Failed to download index from remote")
            
            # Load and analyze index
            index = DatasetIndex.load(index_path)
            
            # Print overall statistics
            click.echo("\nOverall Dataset Statistics:")
            click.echo(f"Total tracks: {len(index.tracks)}")
            click.echo(f"Total artists: {len(index.album_by_artist)}")
            click.echo(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
            click.echo(f"Total files: {index.total_files if hasattr(index, 'total_files') else 'N/A'}") # Handle older index versions
            click.echo(f"Total size: {index.total_size / (1024*1024*1024):.2f} GB")

            # Print per-location statistics if available
            if hasattr(index, 'stats_by_location') and index.stats_by_location:
                click.echo("\nStatistics by Location:")
                for loc_name, loc_stats in sorted(index.stats_by_location.items()):
                    size_gb = loc_stats.get('total_size', 0) / (1024*1024*1024)
                    click.echo(f"  Location: {loc_name}")
                    click.echo(f"    Tracks: {loc_stats.get('track_count', 0)}")
                    click.echo(f"    Artists: {loc_stats.get('artist_count', 0)} {list(loc_stats.get('artists', []))}")
                    click.echo(f"    Albums: {loc_stats.get('album_count', 0)}")
                    click.echo(f"    Files: {loc_stats.get('file_count', 0)}")
                    click.echo(f"    Size: {size_gb:.2f} GB")
            else:
                click.echo("\n(Per-location statistics not available in this index)")

            # Count components and their sizes (overall)
            component_counts = defaultdict(int)
            component_sizes = defaultdict(int)
            
            # Track missing component stats if requested
            missing_stats = defaultdict(int) if missing else None
            missing_artists = set() if missing else None
            missing_albums = set() if missing else None
            
            for track_path, track in index.tracks.items():
                # Count regular components
                for comp_name, file_path in track.files.items():
                    component_counts[comp_name] += 1
                    component_sizes[comp_name] += track.file_sizes[file_path]
                
                # Check for missing component if requested
                if missing and missing not in track.files:
                    # Count other components present in tracks missing the specified one
                    for comp_name in track.files:
                        missing_stats[comp_name] += 1
                    # Track artists and albums with missing files
                    missing_artists.add(track.artist)
                    missing_albums.add(track.album_path)
            
            # Print overall component statistics
            click.echo("\nOverall Component Statistics:")
            if component_counts:
                for comp_name, count in sorted(component_counts.items()):
                    size_gb = component_sizes[comp_name] / (1024*1024*1024)
                    click.echo(f"- {comp_name}: {count} files ({size_gb:.2f} GB)")
            else:
                click.echo("No components found in the index.")
            
            # Show missing component statistics if requested
            if missing:
                total_missing = len(index.tracks) - component_counts.get(missing, 0)
                click.echo(f"\nTracks missing '{missing}' component:")
                click.echo(f"Total tracks without {missing}: {total_missing}")
                click.echo(f"Artists affected: {len(missing_artists)}")
                click.echo(f"Albums affected: {len(missing_albums)}")
                click.echo("\nComponents present in tracks missing this one:")
                for comp_name, count in sorted(missing_stats.items()):
                    click.echo(f"- {comp_name}: {count} files")
            
            # Clean up
            shutil.rmtree(temp_dir)
            
        else:
            # Local path handling
            if not Path(dataset_path).exists():
                raise ValueError(f"Path '{dataset_path}' does not exist")
                
            # Load existing index
            index_path = Path(dataset_path) / ".blackbird" / "index.pickle"
            if not index_path.exists():
                raise ValueError(f"Index not found at {index_path}. Run 'blackbird reindex' first.")
                
            index = DatasetIndex.load(index_path)
            
            # Print overall statistics
            click.echo("\nOverall Dataset Statistics:")
            click.echo(f"Total tracks: {len(index.tracks)}")
            click.echo(f"Total artists: {len(index.album_by_artist)}")
            click.echo(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
            click.echo(f"Total files: {index.total_files if hasattr(index, 'total_files') else 'N/A'}") # Handle older index versions
            click.echo(f"Total size: {index.total_size / (1024*1024*1024):.2f} GB")

            # Print per-location statistics if available
            if hasattr(index, 'stats_by_location') and index.stats_by_location:
                click.echo("\nStatistics by Location:")
                for loc_name, loc_stats in sorted(index.stats_by_location.items()):
                    size_gb = loc_stats.get('total_size', 0) / (1024*1024*1024)
                    click.echo(f"  Location: {loc_name}")
                    click.echo(f"    Tracks: {loc_stats.get('track_count', 0)}")
                    click.echo(f"    Artists: {loc_stats.get('artist_count', 0)} {list(loc_stats.get('artists', []))}")
                    click.echo(f"    Albums: {loc_stats.get('album_count', 0)}")
                    click.echo(f"    Files: {loc_stats.get('file_count', 0)}")
                    click.echo(f"    Size: {size_gb:.2f} GB")
            else:
                click.echo("\n(Per-location statistics not available in this index)")

            # Count components and their sizes (overall)
            component_counts = defaultdict(int)
            component_sizes = defaultdict(int)
            
            # Track missing component stats if requested
            missing_stats = defaultdict(int) if missing else None
            missing_artists = set() if missing else None
            missing_albums = set() if missing else None
            
            for track_path, track in index.tracks.items():
                # Count regular components
                for comp_name, file_path in track.files.items():
                    component_counts[comp_name] += 1
                    component_sizes[comp_name] += track.file_sizes[file_path]
                
                # Check for missing component if requested
                if missing and missing not in track.files:
                    # Count other components present in tracks missing the specified one
                    for comp_name in track.files:
                        missing_stats[comp_name] += 1
                    # Track artists and albums with missing files
                    missing_artists.add(track.artist)
                    missing_albums.add(track.album_path)
            
            # Print overall component statistics
            click.echo("\nOverall Component Statistics:")
            if component_counts:
                for comp_name, count in sorted(component_counts.items()):
                    size_gb = component_sizes[comp_name] / (1024*1024*1024)
                    click.echo(f"- {comp_name}: {count} files ({size_gb:.2f} GB)")
            else:
                click.echo("No components found in the index.")
            
            # Show missing component statistics if requested
            if missing:
                total_missing = len(index.tracks) - component_counts.get(missing, 0)
                click.echo(f"\nTracks missing '{missing}' component:")
                click.echo(f"Total tracks without {missing}: {total_missing}")
                click.echo(f"Artists affected: {len(missing_artists)}")
                click.echo(f"Albums affected: {len(missing_albums)}")
                click.echo("\nComponents present in tracks missing this one:")
                for comp_name, count in sorted(missing_stats.items()):
                    click.echo(f"- {comp_name}: {count} files")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.option('--missing', help='Comma-separated list of components that must be missing')
@click.option('--has', help='Comma-separated list of components that must be present')
@click.option('--artist', help='Filter by artist name')
@click.option('--album', help='Filter by album name')
def find_tracks(dataset_path: str, missing: Optional[str], has: Optional[str], 
                artist: Optional[str], album: Optional[str]):
    """Find tracks based on component presence.
    
    DATASET_PATH: Path to the dataset
    """
    try:
        dataset = Dataset(Path(dataset_path))
        missing_components = missing.split(',') if missing else None
        has_components = has.split(',') if has else None
        
        tracks = dataset.find_tracks(
            missing=missing_components,
            has=has_components,
            artist=artist,
            album=album
        )
        
        if not tracks:
            click.echo("No matching tracks found.")
            return
            
        click.echo(f"\nFound {len(tracks)} matching tracks:")
        for track_id, files in tracks.items():
            click.echo(f"\n{track_id}:")
            for file_path in files:
                click.echo(f"  - {file_path}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.group()
def schema():
    """Schema management commands."""
    pass

@schema.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.option('--num-artists', type=int, default=None, 
             help='Number of random artists to analyze (default: all artists)')
@click.option('--test-run', is_flag=True,
             help='Run in test mode - analyze but do not save schema')
def discover(dataset_path: str, num_artists: Optional[int], test_run: bool):
    """Discover and save schema for a dataset.
    
    DATASET_PATH: Path to the dataset root directory
    """
    try:
        dataset_path = Path(dataset_path)
        
        # Create schema manager
        schema = DatasetComponentSchema(dataset_path)
        
        # Get all artist folders
        artist_folders = [f for f in dataset_path.iterdir() 
                         if f.is_dir() and not f.name.startswith('.')]
        
        # Select artists to analyze
        if num_artists is not None:
            selected_artists = random.sample(artist_folders, min(num_artists, len(artist_folders)))
        else:
            selected_artists = artist_folders
        
        # Convert to relative paths for discovery
        artist_paths = [str(f.relative_to(dataset_path)) for f in selected_artists]
        
        click.echo("Analyzing artists:")
        for path in artist_paths:
            click.echo(f"- {path}")
        
        # Discover schema
        click.echo("\nDiscovering schema...")
        result = schema.discover_schema(folders=artist_paths)
        
        if result.is_valid:
            click.echo("\nSchema discovery successful!")
            
            if not test_run:
                # Ensure .blackbird directory exists
                blackbird_dir = dataset_path / ".blackbird"
                blackbird_dir.mkdir(exist_ok=True)
                
                # Save schema
                schema.save()
            
            # Print discovered schema in a more readable format
            click.echo("\nDiscovered Components:")
            for name, config in schema.schema["components"].items():
                click.echo(f"\n{name}:")
                click.echo(f"  Pattern: {config['pattern']}")
                click.echo(f"  Multiple: {config['multiple']}")
                if "description" in config:
                    click.echo(f"  Description: {config['description']}")
                    
                # Print corresponding statistics
                if name in result.stats["components"]:
                    stats = result.stats["components"][name]
                    click.echo("\n  Statistics:")
                    click.echo(f"    Files found: {stats['file_count']}")
                    click.echo(f"    Track coverage: {stats['track_coverage']*100:.1f}%")
                    click.echo(f"    Unique tracks: {stats['unique_tracks']}")
                    click.echo(f"    Has sections: {stats['has_sections']}")
            
            if not test_run:
                click.echo(f"\nSchema saved to {schema.schema_path}")
            else:
                click.echo("\nTest run completed - schema was not saved")
        else:
            click.echo("\nSchema discovery failed with errors:")
            for error in result.errors:
                click.echo(f"- {error}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@schema.command()
@click.argument('dataset_path')
def show(dataset_path: str):
    """Show current schema.
    
    DATASET_PATH: Path to the dataset or WebDAV URL
    """
    try:
        # Check if it's a WebDAV URL
        if dataset_path.startswith(('http://', 'https://', 'webdav://')):
            # Create a fixed temporary directory for downloading schema
            temp_dir = '/tmp/blackbird_schema_temp'
            temp_path = Path(temp_dir)
            temp_path.mkdir(parents=True, exist_ok=True)
            
            # Configure WebDAV client
            client = configure_client(dataset_path)
            
            # Download schema
            click.echo("Downloading schema from remote...")
            schema_path = temp_path / '.blackbird' / 'schema.json'
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            
            if not client.download_file('.blackbird/schema.json', schema_path):
                raise ValueError("Failed to download schema from remote")
            
            # Load and show schema
            schema = DatasetComponentSchema(temp_path)
            click.echo("\nRemote Schema:")
            click.echo(json.dumps(schema.schema, indent=2))
            click.echo(f"\nSchema downloaded to: {temp_dir}")
        else:
            # Local path handling
            if not Path(dataset_path).exists():
                raise ValueError(f"Path '{dataset_path}' does not exist")
                
            schema = DatasetComponentSchema(Path(dataset_path))
            click.echo("\nLocal Schema:")
            click.echo(json.dumps(schema.schema, indent=2))
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@schema.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.argument('name')
@click.argument('pattern')
@click.option('--multiple', is_flag=True, help='Allow multiple files of this type per track')
def add(dataset_path: str, name: str, pattern: str, multiple: bool):
    """Add new component to schema.
    
    DATASET_PATH: Path to the dataset
    NAME: Component name/identifier
    PATTERN: Glob pattern for matching files
    """
    try:
        schema = DatasetComponentSchema(Path(dataset_path))
        result = schema.add_component(name, pattern, multiple=multiple)
        
        if result.is_valid:
            click.echo(f"Successfully added component '{name}'")
            schema.save()
        else:
            click.echo("Failed to add component:")
            for error in result.errors:
                click.echo(f"- {error}")
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.command()
@click.argument('dataset_path', type=click.Path(exists=True))
def reindex(dataset_path: str):
    """Rebuild dataset index.
    
    DATASET_PATH: Path to the dataset
    """
    try:
        click.echo("Rebuilding dataset index...")
        dataset = Dataset(Path(dataset_path))
        dataset.rebuild_index()
        
        # Calculate component statistics
        component_counts = defaultdict(int)
        component_sizes = defaultdict(int)
        for track in dataset._index.tracks.values():
            for comp_name, file_path in track.files.items():
                component_counts[comp_name] += 1
                component_sizes[comp_name] += track.file_sizes[file_path]
        
        # Show statistics
        click.echo("\nIndex rebuilt successfully!")
        click.echo(f"\nNew index statistics:")
        click.echo(f"Total tracks: {len(dataset._index.tracks)}")
        click.echo(f"Total artists: {len(dataset._index.album_by_artist)}")
        click.echo(f"Total albums: {sum(len(albums) for albums in dataset._index.album_by_artist.values())}")
        click.echo(f"\nComponents indexed:")
        for comp_name in sorted(component_counts.keys()):
            count = component_counts[comp_name]
            size_gb = component_sizes[comp_name] / (1024*1024*1024)
            click.echo(f"  {comp_name}: {count} files ({size_gb:.2f} GB)")
        
        # Show where index was saved
        index_path = Path(dataset_path) / ".blackbird" / "index.pickle"
        click.echo(f"\nIndex saved to: {index_path}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@main.group()
def webdav():
    """WebDAV server management commands."""
    pass

@webdav.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.option('--port', type=int, required=True, help='Port for WebDAV server')
@click.option('--username', help='WebDAV username')
@click.option('--password', help='WebDAV password')
@click.option('--non-interactive', is_flag=True, help='Run without prompts')
def setup(dataset_path: str, port: int, username: Optional[str], password: Optional[str], 
         non_interactive: bool):
    """Setup WebDAV server for dataset sharing."""
    from .webdav import WebDAVSetup
    
    wizard = WebDAVSetup(
        dataset_path=Path(dataset_path),
        port=port,
        username=username,
        password=password,
        non_interactive=non_interactive
    )
    
    if not wizard.run():
        sys.exit(1)

@webdav.command()
def list():
    """List WebDAV shares created by Blackbird."""
    from .webdav import WebDAVSetup
    
    shares = WebDAVSetup.list_shares()
    if not shares:
        click.echo("No Blackbird WebDAV shares found")
        return
        
    click.echo("\nFound WebDAV shares:")
    for share in shares:
        click.echo(f"\nPort: {share.port}")
        click.echo(f"Path: {share.path}")
        click.echo(f"Status: {'Active' if share.is_running() else 'Inactive'}")
        click.echo(f"Config: {share.config_path}")

@main.group()
def location():
    """Manage dataset storage locations."""
    pass

def _get_locations_manager(dataset_path_str: str) -> LocationsManager:
    """Helper to instantiate LocationsManager and handle common errors."""
    dataset_path = Path(dataset_path_str).resolve()
    if not dataset_path.is_dir():
        click.echo(f"Error: Dataset path '{dataset_path}' does not exist or is not a directory.", err=True)
        sys.exit(1)
    
    blackbird_dir = dataset_path / ".blackbird"
    if not blackbird_dir.exists():
         # Allow commands like list/add even if .blackbird doesn't exist yet, 
         # LocationsManager handles default creation in memory
         pass
         # click.echo(f"Error: Dataset directory '{dataset_path}' does not seem to be initialized (missing .blackbird folder).", err=True)
         # sys.exit(1)
         
    try:
        manager = LocationsManager(dataset_path)
        manager.load_locations() # Load existing or default
        return manager
    except (ValueError, LocationValidationError) as e:
        click.echo(f"Error initializing locations manager: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"An unexpected error occurred: {e}", err=True)
        sys.exit(1)

@location.command('list')
@click.argument('dataset_path', type=click.Path(file_okay=False, resolve_path=True))
def list_locations(dataset_path: str):
    """List all configured storage locations."""
    manager = _get_locations_manager(dataset_path)
    try:
        locations = manager.get_all_locations()
        if not locations:
            click.echo("No locations configured (using default 'Main').")
            # Attempt to show default if manager loaded it
            try:
                main_path = manager.get_location_path(LocationsManager.DEFAULT_LOCATION_NAME)
                click.echo(f"  {LocationsManager.DEFAULT_LOCATION_NAME}: {main_path}")
            except KeyError:
                 click.echo(f"  Default location '{LocationsManager.DEFAULT_LOCATION_NAME}' points to: {Path(dataset_path).resolve()}")
            return

        click.echo("Configured locations:")
        max_name_len = max(len(name) for name in locations.keys()) if locations else 0
        for name, path in sorted(locations.items()):
            click.echo(f"  {name:<{max_name_len}} : {path}")
            
    except Exception as e:
        click.echo(f"Error listing locations: {e}", err=True)
        sys.exit(1)

@location.command('add')
@click.argument('dataset_path', type=click.Path(file_okay=False, resolve_path=True))
@click.argument('name')
@click.argument('location_path', type=click.Path(file_okay=False, resolve_path=True))
def add_location(dataset_path: str, name: str, location_path: str):
    """Adds a new storage location and saves the configuration."""
    try:
        lm = _get_locations_manager(dataset_path)
        # location_path is already resolved by click.Path, pass as string
        lm.add_location(name, str(location_path))
        lm.save_locations()
        click.echo(f"Location '{name}' added successfully.")
    except LocationValidationError as e:
        click.echo(f"Error adding location: {e}", err=True)
        sys.exit(1)
    except Exception as e: # Catch unexpected errors during add/save
        click.echo(f"An unexpected error occurred while adding location: {e}", err=True)
        sys.exit(1)

@location.command('remove')
@click.argument('dataset_path', type=click.Path(file_okay=False, resolve_path=True))
@click.argument('name')
@click.confirmation_option(prompt='Are you sure you want to remove this location? This does NOT delete data.')
def remove_location(dataset_path: str, name: str):
    """Remove a storage location configuration (does not delete data)."""
    try:
        manager = _get_locations_manager(dataset_path)
        manager.remove_location(name)
        manager.save_locations()
        click.echo(f"{Fore.GREEN}Location '{name}' removed successfully.")
    except (ValueError, KeyError, LocationValidationError) as e:
        click.echo(f"{Fore.RED}Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error removing location: {e}")
        click.echo(f"{Fore.RED}An unexpected error occurred: {e}", err=True)
        sys.exit(1)

@location.command('balance')
@click.argument('dataset_path', type=click.Path(exists=True, file_okay=False, resolve_path=True))
@click.argument('source_loc')
@click.argument('target_loc')
@click.option('--size', type=float, required=True, help='Approximate size in GB to move.')
@click.option('--dry-run', is_flag=True, help='Simulate the move without moving files.')
@click.confirmation_option(prompt='Are you sure you want to move data between these locations?')
def balance_location(dataset_path: str, source_loc: str, target_loc: str, size: float, dry_run: bool):
    """Balance storage by moving data between locations to reach a target size."""
    try:
        dataset_path_obj = Path(dataset_path)
        dataset = Dataset(dataset_path_obj)
        # dataset.load_index() # REMOVED: Index is loaded on Dataset init if available

        if dataset.index is None:
             click.echo(f"{Fore.RED}Error: Dataset index not found or failed to load at {dataset.index_path}. Please run 'reindex' first.", err=True)
             sys.exit(1)

        click.echo(f"Attempting to move approximately {size:.2f} GB from '{source_loc}' to '{target_loc}'...")

        move_stats = move_data(
            dataset=dataset,
            source_location_name=source_loc,
            target_location_name=target_loc,
            size_limit_gb=size,
            dry_run=dry_run,
        )

        if dry_run:
            click.echo(f"\n{Fore.YELLOW}Dry run complete.")
            click.echo(f"Would have skipped {move_stats['skipped_files']} files.")
        else:
            click.echo(f"\n{Fore.GREEN}Move operation complete!")
            click.echo(f"Moved {move_stats['moved_files']} files ({format_size(move_stats['total_bytes_moved'])}).")
            click.echo(f"Skipped {move_stats['skipped_files']} files.")
            click.echo(f"Failed {move_stats['failed_files']} files.")
            # Trigger re-index after successful move
            click.echo("Rebuilding index...")
            dataset.rebuild_index()
            click.echo("Index rebuilt.")


    except (ValueError, KeyError, LocationValidationError, SymbolicPathError) as e:
        click.echo(f"{Fore.RED}Error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e: # Catch index loading errors specifically
        click.echo(f"{Fore.RED}Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error during balance operation: {e}")
        click.echo(f"{Fore.RED}An unexpected error occurred: {e}", err=True)
        sys.exit(1)

@location.command('move-folders')
@click.argument('dataset_path', type=click.Path(exists=True, file_okay=False, resolve_path=True))
@click.argument('target_loc')
@click.argument('folders', nargs=-1, required=True)
@click.option('--source-location', required=True, help='Name of the source location to move folders from.')
@click.option('--dry-run', is_flag=True, help='Simulate the move without moving files.')
@click.confirmation_option(prompt='Are you sure you want to move these specific folders?')
def move_location_folders(dataset_path: str, target_loc: str, folders: List[str], source_location: str, dry_run: bool):
    """Move specific folders (relative to source location root) to another location."""
    try:
        dataset_path_obj = Path(dataset_path)
        dataset = Dataset(dataset_path_obj)
        # dataset.load_index() # REMOVED: Index is loaded on Dataset init if available

        if dataset.index is None:
             click.echo(f"{Fore.RED}Error: Dataset index not found or failed to load at {dataset.index_path}. Please run 'reindex' first.", err=True)
             sys.exit(1)

        click.echo(f"Attempting to move folders {folders} from '{source_location}' to '{target_loc}'...")

        move_stats = move_data(
            dataset=dataset,
            source_location_name=source_location,
            target_location_name=target_loc,
            specific_folders=list(folders),
            dry_run=dry_run,
        )

        if dry_run:
            click.echo(f"\n{Fore.YELLOW}Dry run complete.")
            click.echo(f"Would have skipped {move_stats['skipped_files']} files.")
        else:
            click.echo(f"\n{Fore.GREEN}Move operation complete!")
            click.echo(f"Moved {move_stats['moved_files']} files ({format_size(move_stats['total_bytes_moved'])}).")
            click.echo(f"Skipped {move_stats['skipped_files']} files.")
            click.echo(f"Failed {move_stats['failed_files']} files.")
            # Trigger re-index after successful move
            click.echo("Rebuilding index...")
            dataset.rebuild_index()
            click.echo("Index rebuilt.")

    except (ValueError, KeyError, LocationValidationError, SymbolicPathError) as e:
        click.echo(f"{Fore.RED}Error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e: # Catch index loading errors specifically
        click.echo(f"{Fore.RED}Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error during move-folders operation: {e}")
        click.echo(f"{Fore.RED}An unexpected error occurred: {e}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
