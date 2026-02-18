#!/usr/bin/env python3
"""Blackbird Dataset — Python API usage examples.

This script creates a self-contained temporary dataset and demonstrates
every major feature of the Blackbird Python API:

  1. Schema creation and component management
  2. Automatic schema discovery from files on disk
  3. Dataset initialization and index building
  4. Dataset analysis (statistics)
  5. Track search — by component presence, artist, album
  6. Index search — by artist, album, track name (with fuzzy search)
  7. Schema validation
  8. Locations management (multi-location datasets)
  9. Path resolution (symbolic → absolute)
 10. Rebuilding the index after adding new files

All operations run inside a temporary directory that is cleaned up
automatically when the script exits.
"""

import json
import tempfile
from pathlib import Path
from collections import defaultdict

from blackbird.dataset import Dataset
from blackbird.schema import DatasetComponentSchema
from blackbird.index import DatasetIndex, TrackInfo
from blackbird.locations import LocationsManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def heading(title: str) -> None:
    """Print a section heading."""
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


def format_size(size_bytes: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def create_dummy_dataset(root: Path) -> Path:
    """Build a realistic dummy dataset on disk.

    Structure created::

        root/
          Artist_A/
            Album_X [2020]/
              01.Artist_A - Song_One_instrumental.mp3
              01.Artist_A - Song_One_vocals_noreverb.mp3
              01.Artist_A - Song_One_vocals_noreverb.json
              01.Artist_A - Song_One.mir.json
              01.Artist_A - Song_One_caption.txt
              01.Artist_A - Song_One_vocals_stretched_120bpm_section1.mp3
              01.Artist_A - Song_One_vocals_stretched_120bpm_section2.mp3
              02.Artist_A - Song_Two_...
              03.Artist_A - Song_Three_...
            Album_Y [2022]/
              01. ...  02. ...
          Artist_B/
            Album_Z [2021]/
              CD1/
                01. ...  02. ...
              CD2/
                01. ...  02. ...
          Artist_C/
            Album_W [2023]/
              01. ...  02. ...  03. ...  04. ...

    Returns the dataset root path.
    """
    # Component suffixes — every track gets each of these files
    suffixes = [
        "_instrumental.mp3",
        "_vocals_noreverb.mp3",
        "_vocals_noreverb.json",
        ".mir.json",
        "_caption.txt",
        "_vocals_stretched_120bpm_section1.mp3",
        "_vocals_stretched_120bpm_section2.mp3",
    ]

    # Artist A — two regular albums
    for album, tracks in [
        ("Album_X [2020]", ["Song_One", "Song_Two", "Song_Three"]),
        ("Album_Y [2022]", ["Song_Four", "Song_Five"]),
    ]:
        album_dir = root / "Artist_A" / album
        album_dir.mkdir(parents=True)
        for i, track in enumerate(tracks, 1):
            base = f"{i:02d}.Artist_A - {track}"
            for sfx in suffixes:
                (album_dir / f"{base}{sfx}").write_bytes(b"\x00" * (500 + i * 100))

    # Artist B — one multi-CD album
    for cd, tracks in [
        ("CD1", ["Intro", "Main_Theme"]),
        ("CD2", ["Bonus_Track", "Hidden_Gem"]),
    ]:
        cd_dir = root / "Artist_B" / "Album_Z [2021]" / cd
        cd_dir.mkdir(parents=True)
        for i, track in enumerate(tracks, 1):
            base = f"{i:02d}.Artist_B - {track}"
            for sfx in suffixes:
                (cd_dir / f"{base}{sfx}").write_bytes(b"\x00" * (400 + i * 80))

    # Artist C — one album, four tracks, intentionally missing some components
    album_dir = root / "Artist_C" / "Album_W [2023]"
    album_dir.mkdir(parents=True)
    for i, track in enumerate(["Alpha", "Beta", "Gamma", "Delta"], 1):
        base = f"{i:02d}.Artist_C - {track}"
        # All tracks get instrumental and mir
        (album_dir / f"{base}_instrumental.mp3").write_bytes(b"\x00" * 700)
        (album_dir / f"{base}.mir.json").write_bytes(b"{}" * 10)
        # Only first two tracks get vocals
        if i <= 2:
            (album_dir / f"{base}_vocals_noreverb.mp3").write_bytes(b"\x00" * 600)
            (album_dir / f"{base}_vocals_noreverb.json").write_bytes(b"{}" * 5)

    return root


def create_locations(dataset_path: Path) -> None:
    """Create the initial locations.json pointing Main to the dataset root."""
    bb = dataset_path / ".blackbird"
    bb.mkdir(parents=True, exist_ok=True)
    locations = {"Main": str(dataset_path)}
    (bb / "locations.json").write_text(json.dumps(locations))


def create_schema(dataset_path: Path) -> DatasetComponentSchema:
    """Create and save a schema with all component definitions."""
    schema = DatasetComponentSchema.create(dataset_path)
    schema.schema["components"].update({
        "instrumental.mp3": {
            "pattern": "*_instrumental.mp3",
            "multiple": False,
        },
        "vocals_noreverb.mp3": {
            "pattern": "*_vocals_noreverb.mp3",
            "multiple": False,
        },
        "vocals_noreverb.json": {
            "pattern": "*_vocals_noreverb.json",
            "multiple": False,
        },
        "mir.json": {
            "pattern": "*.mir.json",
            "multiple": False,
        },
        "caption.txt": {
            "pattern": "*_caption.txt",
            "multiple": False,
        },
        "vocals_stretched_120bpm_section*.mp3": {
            "pattern": "*_vocals_stretched_120bpm_section*.mp3",
            "multiple": True,  # multiple sections per track
        },
    })
    schema.save()
    return schema


# ---------------------------------------------------------------------------
# 1. Schema creation and inspection
# ---------------------------------------------------------------------------

def demo_schema(dataset_path: Path) -> None:
    heading("1. Schema Creation and Inspection")

    create_locations(dataset_path)
    schema = create_schema(dataset_path)
    print(f"Schema saved to: {schema.schema_path}\n")

    # Print each component with its settings
    print("Registered components:")
    for name, cfg in schema.schema["components"].items():
        multi = cfg.get("multiple", False)
        print(f"  - {name}")
        print(f"      pattern  : {cfg['pattern']}")
        print(f"      multiple : {multi}")


# ---------------------------------------------------------------------------
# 2. Schema discovery (auto-detect components from files)
# ---------------------------------------------------------------------------

def demo_schema_discovery(dataset_path: Path) -> None:
    heading("2. Automatic Schema Discovery")

    # Start with a fresh schema (no components defined)
    fresh = DatasetComponentSchema.create(dataset_path)

    # Discover components from an existing album on disk
    result = fresh.discover_schema(folders=["Artist_A/Album_X [2020]"])

    print(f"Discovery valid : {result.is_valid}")
    print(f"Components found: {len(fresh.schema['components'])}\n")

    for name, cfg in fresh.schema["components"].items():
        stats = result.stats["components"].get(name, {})
        print(f"  - {name}")
        print(f"      pattern      : {cfg['pattern']}")
        print(f"      multiple     : {cfg.get('multiple', False)}")
        print(f"      file_count   : {stats.get('file_count', '?')}")
        print(f"      track_coverage: {stats.get('track_coverage', '?')}")

    # Restore the hand-crafted schema for later demos
    create_schema(dataset_path)


# ---------------------------------------------------------------------------
# 3. Dataset initialization and index building
# ---------------------------------------------------------------------------

def demo_dataset_init(dataset_path: Path) -> Dataset:
    heading("3. Dataset Initialization")

    # Dataset() auto-builds an index from all configured locations
    dataset = Dataset(dataset_path)

    print(f"Dataset path : {dataset.path}")
    print(f"Total tracks : {len(dataset.index.tracks)}")
    print(f"Total artists: {len(dataset.index.album_by_artist)}")
    total_albums = sum(len(a) for a in dataset.index.album_by_artist.values())
    print(f"Total albums : {total_albums}")
    print(f"Total size   : {format_size(dataset.index.total_size)}")

    return dataset


# ---------------------------------------------------------------------------
# 4. Dataset analysis (statistics)
# ---------------------------------------------------------------------------

def demo_analyze(dataset: Dataset) -> None:
    heading("4. Dataset Analysis")

    stats = dataset.analyze()

    print(f"Total tracks   : {stats['tracks']['total']}")
    print(f"Complete tracks : {stats['tracks']['complete']}  "
          f"(have every component in the schema)")
    print(f"Total size     : {format_size(stats['total_size'])}")

    # Per-component coverage
    print("\nComponent coverage:")
    total = stats["tracks"]["total"]
    for comp, info in sorted(stats["components"].items()):
        pct = (info["count"] / total * 100) if total else 0
        print(f"  {comp:50s} {info['count']:4d} tracks  ({pct:5.1f}%)  "
              f"size={format_size(info['size'])}")

    # Per-artist track counts
    print("\nTracks per artist:")
    for artist, count in sorted(stats["tracks"]["by_artist"].items()):
        albums = stats["albums"].get(artist, set())
        print(f"  {artist:20s} {count:3d} tracks across {len(albums)} album(s)")


# ---------------------------------------------------------------------------
# 5. Finding tracks by component / artist / album
# ---------------------------------------------------------------------------

def demo_find_tracks(dataset: Dataset) -> None:
    heading("5. Finding Tracks")

    # 5a. All tracks
    all_tracks = dataset.find_tracks()
    print(f"All tracks: {len(all_tracks)}\n")

    # 5b. Tracks that have vocals
    with_vocals = dataset.find_tracks(has=["vocals_noreverb.mp3"])
    print(f"Tracks with vocals: {len(with_vocals)}")
    for tp in sorted(with_vocals)[:3]:
        print(f"  {tp}")

    # 5c. Tracks missing vocals
    missing_vocals = dataset.find_tracks(missing=["vocals_noreverb.mp3"])
    print(f"\nTracks missing vocals: {len(missing_vocals)}")
    for tp in sorted(missing_vocals):
        print(f"  {tp}")

    # 5d. Tracks that have both vocals AND caption
    complete = dataset.find_tracks(has=["vocals_noreverb.mp3", "caption.txt"])
    print(f"\nTracks with vocals + caption: {len(complete)}")

    # 5e. Filter by artist
    artist_b = dataset.find_tracks(artist="Artist_B")
    print(f"\nArtist_B tracks: {len(artist_b)}")
    for tp in sorted(artist_b):
        print(f"  {tp}")

    # 5f. Filter by artist + component
    artist_c_with_vocals = dataset.find_tracks(
        has=["vocals_noreverb.mp3"],
        artist="Artist_C",
    )
    print(f"\nArtist_C tracks with vocals: {len(artist_c_with_vocals)}")
    for tp in sorted(artist_c_with_vocals):
        print(f"  {tp}")

    # 5g. CD album tracks
    cd_tracks = [tp for tp in all_tracks if "/CD" in tp]
    print(f"\nCD album tracks: {len(cd_tracks)}")
    for tp in sorted(cd_tracks):
        print(f"  {tp}")


# ---------------------------------------------------------------------------
# 6. Index search (by artist / album / track name)
# ---------------------------------------------------------------------------

def demo_index_search(dataset: Dataset) -> None:
    heading("6. Index Search")

    idx = dataset.index

    # 6a. Search by artist (substring match)
    print("search_by_artist('Artist'):")
    for a in idx.search_by_artist("Artist"):
        print(f"  {a}")

    # 6b. Search by artist — case insensitive (default)
    print("\nsearch_by_artist('artist_a'):")
    for a in idx.search_by_artist("artist_a"):
        print(f"  {a}")

    # 6c. Fuzzy artist search (when exact match fails)
    print("\nsearch_by_artist('Artst_B', fuzzy_search=True):")
    for a in idx.search_by_artist("Artst_B", fuzzy_search=True):
        print(f"  {a}")

    # 6d. Search albums
    print("\nsearch_by_album('Album'):")
    for alb in idx.search_by_album("Album"):
        print(f"  {alb}")

    # 6e. Search albums for a specific artist
    print("\nsearch_by_album('Album', artist='Artist_A'):")
    for alb in idx.search_by_album("Album", artist="Artist_A"):
        print(f"  {alb}")

    # 6f. Search tracks by base name
    # Note: base names are derived by stripping component suffixes from
    # filenames.  With "01.Artist_A - Song_One_instrumental.mp3" the
    # indexer uses Path.stem, which yields "01" as the base name.
    print("\nsearch_by_track('01')  — matches all track-1 across artists:")
    for t in idx.search_by_track("01"):
        print(f"  {t.track_path}  (base_name={t.base_name})")

    # 6g. Search tracks within a specific artist
    print("\nsearch_by_track('', artist='Artist_C')  — all Artist_C tracks:")
    for t in idx.search_by_track("", artist="Artist_C"):
        print(f"  {t.track_path}  (base_name={t.base_name})")

    # 6h. Get all component files for a specific track
    some_track = next(iter(idx.tracks))
    print(f"\nget_track_files('{some_track}'):")
    for comp, fpath in idx.get_track_files(some_track).items():
        print(f"  {comp:50s} -> {fpath}")


# ---------------------------------------------------------------------------
# 7. Schema validation
# ---------------------------------------------------------------------------

def demo_validation(dataset: Dataset) -> None:
    heading("7. Schema Validation")

    result = dataset.validate()
    print(f"Valid    : {result.is_valid}")
    print(f"Errors   : {len(result.errors)}")
    print(f"Warnings : {len(result.warnings)}")

    if result.errors:
        print("\nErrors:")
        for e in result.errors:
            print(f"  - {e}")
    if result.warnings:
        print("\nWarnings:")
        for w in result.warnings[:5]:
            print(f"  - {w}")

    # validate_against_data checks a specific folder against the schema
    schema = dataset.schema
    vr = schema.validate_against_data(dataset.path / "Artist_A" / "Album_X [2020]")
    print(f"\nValidation of Album_X [2020]:")
    print(f"  valid          : {vr.is_valid}")
    print(f"  unmatched_files: {vr.stats.get('unmatched_files', 'N/A')}")


# ---------------------------------------------------------------------------
# 8. Locations management (multi-location datasets)
# ---------------------------------------------------------------------------

def demo_locations(dataset: Dataset, tmp_root: Path) -> Dataset:
    heading("8. Locations Management")

    lm = dataset.locations

    # Show current locations
    print("Current locations:")
    for name, path in lm.get_all_locations().items():
        print(f"  {name:10s} -> {path}")

    # Create a second storage location with some data
    backup_dir = tmp_root / "backup_location"
    (backup_dir / "Artist_D" / "Album_V [2024]").mkdir(parents=True)
    for i, track in enumerate(["Rain", "Snow"], 1):
        base = f"{i:02d}.Artist_D - {track}"
        (backup_dir / "Artist_D" / "Album_V [2024]" / f"{base}_instrumental.mp3").write_bytes(b"\x00" * 900)
        (backup_dir / "Artist_D" / "Album_V [2024]" / f"{base}.mir.json").write_bytes(b"{}" * 10)

    # Add the new location and persist to locations.json
    lm.add_location("Backup", str(backup_dir))
    lm.save_locations()
    print(f"\nAdded location 'Backup' -> {backup_dir}")

    print("\nUpdated locations:")
    for name, path in lm.get_all_locations().items():
        print(f"  {name:10s} -> {path}")

    # Rebuild index to pick up the new location
    dataset.rebuild_index()
    print(f"\nAfter rebuild: {len(dataset.index.tracks)} tracks "
          f"({len(dataset.index.album_by_artist)} artists)")

    # Per-location stats from the index
    print("\nPer-location stats:")
    for loc, st in dataset.index.stats_by_location.items():
        print(f"  {loc:10s}  tracks={st['track_count']}  "
              f"files={st['file_count']}  "
              f"size={format_size(st['total_size'])}")

    # Remove the backup location and persist the change
    lm.remove_location("Backup")
    lm.save_locations()
    print(f"\nRemoved location 'Backup'")
    print("Locations after removal:")
    for name, path in lm.get_all_locations().items():
        print(f"  {name:10s} -> {path}")

    # Rebuild to reflect the removal
    dataset.rebuild_index()
    return dataset


# ---------------------------------------------------------------------------
# 9. Path resolution (symbolic -> absolute)
# ---------------------------------------------------------------------------

def demo_path_resolution(dataset: Dataset) -> None:
    heading("9. Symbolic Path Resolution")

    # Pick a few tracks and resolve their file paths
    for track_path, track_info in list(dataset.index.tracks.items())[:3]:
        print(f"Track: {track_path}")
        for comp, symbolic in track_info.files.items():
            resolved = dataset.resolve_path(symbolic)
            print(f"  {comp:50s}")
            print(f"    symbolic : {symbolic}")
            print(f"    resolved : {resolved}")
        print()


# ---------------------------------------------------------------------------
# 10. Rebuilding the index after adding new files
# ---------------------------------------------------------------------------

def demo_rebuild(dataset: Dataset) -> None:
    heading("10. Rebuilding Index After New Files")

    print(f"Tracks before: {len(dataset.index.tracks)}")

    # Add a brand-new track to an existing album
    new_track_dir = dataset.path / "Artist_A" / "Album_X [2020]"
    base = "04.Artist_A - Song_Six"
    (new_track_dir / f"{base}_instrumental.mp3").write_bytes(b"\x00" * 800)
    (new_track_dir / f"{base}.mir.json").write_bytes(b"{}" * 10)
    print(f"Created new track files: {base}")

    dataset.rebuild_index()
    print(f"Tracks after : {len(dataset.index.tracks)}")

    # Verify the new track is in the index
    new_key = [k for k in dataset.index.tracks if "Song_Six" in k]
    if new_key:
        print(f"New track found: {new_key[0]}")
        for comp, fpath in dataset.index.get_track_files(new_key[0]).items():
            print(f"  {comp} -> {fpath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    with tempfile.TemporaryDirectory(prefix="blackbird_example_") as tmp:
        tmp_root = Path(tmp)
        dataset_path = tmp_root / "demo_dataset"
        dataset_path.mkdir()

        # Populate dummy files on disk
        create_dummy_dataset(dataset_path)

        # Run all demos in order
        demo_schema(dataset_path)
        demo_schema_discovery(dataset_path)
        dataset = demo_dataset_init(dataset_path)
        demo_analyze(dataset)
        demo_find_tracks(dataset)
        demo_index_search(dataset)
        demo_validation(dataset)
        dataset = demo_locations(dataset, tmp_root)
        demo_path_resolution(dataset)
        demo_rebuild(dataset)

        print(f"\n{'=' * 72}")
        print("  All examples completed successfully!")
        print(f"{'=' * 72}\n")


if __name__ == "__main__":
    main()
