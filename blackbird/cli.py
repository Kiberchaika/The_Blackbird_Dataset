import click
from pathlib import Path
from typing import List, Optional
from .dataset import Dataset
from .schema import DatasetComponentSchema
from .sync import clone_dataset, SyncStats, configure_client
from .index import DatasetIndex
import json
import sys
import tempfile
import shutil
from collections import defaultdict
import random

@click.group()
def main():
    """Blackbird Dataset Manager CLI"""
    pass

@main.command()
@click.argument('source')
@click.argument('destination')
@click.option('--components', help='Comma-separated list of components to clone')
@click.option('--missing', help='Only clone components for tracks missing this component')
@click.option('--artists', help='Comma-separated list of artists to clone (supports glob patterns)')
@click.option('--proportion', type=float, help='Proportion of dataset to clone (0-1)')
@click.option('--offset', type=int, default=0, help='Offset for proportion-based cloning')
def clone(source: str, destination: str, components: Optional[str], missing: Optional[str],
         artists: Optional[str], proportion: Optional[float], offset: Optional[int]):
    """Clone dataset from remote source.
    
    SOURCE: Remote dataset URL (e.g. webdav://server/dataset)
    DESTINATION: Local path for the cloned dataset
    """
    try:
        # Convert comma-separated strings to lists
        component_list = components.split(',') if components else None
        artist_list = artists.split(',') if artists else None
        
        # Validate proportion
        if proportion is not None and not (0 < proportion <= 1):
            raise ValueError("Proportion must be between 0 and 1")
            
        click.echo(f"Cloning from {source} to {destination}")
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
            destination=Path(destination),
            components=component_list,
            missing_component=missing,
            artists=artist_list,
            proportion=proportion,
            offset=offset
        )
        
        # Print summary
        click.echo("\nClone completed!")
        click.echo(f"Total files: {stats.total_files}")
        click.echo(f"Downloaded: {stats.downloaded_files}")
        click.echo(f"Failed: {stats.failed_files}")
        click.echo(f"Total size: {stats.total_size / (1024*1024*1024):.2f} GB")
        click.echo(f"Downloaded size: {stats.downloaded_size / (1024*1024*1024):.2f} GB")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
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
            
            # Count components and their sizes
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
            
            # Print statistics
            click.echo("\nDataset Statistics:")
            click.echo(f"Total tracks: {len(index.tracks)}")
            click.echo(f"Total artists: {len(index.album_by_artist)}")
            click.echo(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
            
            click.echo("\nComponents:")
            for comp_name, count in sorted(component_counts.items()):
                size_gb = component_sizes[comp_name] / (1024*1024*1024)
                click.echo(f"- {comp_name}: {count} files ({size_gb:.2f} GB)")
            
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
            
            # Count components and their sizes
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
            
            # Print statistics
            click.echo("\nDataset Statistics:")
            click.echo(f"Total tracks: {len(index.tracks)}")
            click.echo(f"Total artists: {len(index.album_by_artist)}")
            click.echo(f"Total albums: {sum(len(albums) for albums in index.album_by_artist.values())}")
            
            click.echo("\nComponents:")
            for comp_name, count in sorted(component_counts.items()):
                size_gb = component_sizes[comp_name] / (1024*1024*1024)
                click.echo(f"- {comp_name}: {count} files ({size_gb:.2f} GB)")
            
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

if __name__ == '__main__':
    main()
