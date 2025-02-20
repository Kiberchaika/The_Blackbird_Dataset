import click
from pathlib import Path
from typing import List, Optional
from .dataset import Dataset
from .schema import DatasetComponentSchema
from .sync import clone_dataset, SyncStats, configure_client
import json
import sys
import tempfile

@click.group()
def main():
    """Blackbird Dataset Manager CLI"""
    pass

@main.command()
@click.argument('source')
@click.argument('destination')
@click.option('--components', help='Comma-separated list of components to clone')
@click.option('--artists', help='Comma-separated list of artists to clone (supports glob patterns)')
@click.option('--proportion', type=float, help='Proportion of dataset to clone (0-1)')
@click.option('--offset', type=int, default=0, help='Offset for proportion-based cloning')
def clone(source: str, destination: str, components: Optional[str], artists: Optional[str], 
         proportion: Optional[float], offset: Optional[int]):
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
        if artist_list:
            click.echo(f"Artists: {', '.join(artist_list)}")
        if proportion:
            click.echo(f"Proportion: {proportion} (offset: {offset})")
            
        # Clone dataset
        stats = clone_dataset(
            source_url=source,
            destination=Path(destination),
            components=component_list,
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
@click.argument('dataset_path', type=click.Path(exists=True))
def stats(dataset_path: str):
    """Show dataset statistics.
    
    DATASET_PATH: Path to the dataset
    """
    try:
        dataset = Dataset(Path(dataset_path))
        # Force rebuild index to ensure we have current data
        dataset.rebuild_index()
        stats_result = dataset.analyze()
        
        click.echo("\nDataset Statistics:")
        click.echo(f"Total tracks: {stats_result['tracks']['total']}")
        click.echo(f"Complete tracks: {stats_result['tracks']['complete']}")
        click.echo("\nComponents:")
        for component, count in stats_result['components'].items():
            click.echo(f"- {component}: {count} files")
        click.echo("\nArtists:")
        for artist in sorted(stats_result['artists']):
            click.echo(f"- {artist}")
            for album in sorted(stats_result['albums'][artist]):
                click.echo(f"  - {album}")
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
        click.echo("Index rebuilt successfully!")
        
        # Show some stats about the new index
        stats = dataset.analyze()
        click.echo(f"\nNew index statistics:")
        click.echo(f"Total tracks: {stats['tracks']['total']}")
        click.echo(f"Total artists: {len(stats['artists'])}")
        click.echo(f"Total albums: {sum(len(albums) for albums in stats['albums'].values())}")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
