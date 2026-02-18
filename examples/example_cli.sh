#!/bin/bash
# ==========================================================================
#  Blackbird Dataset — CLI usage examples
#
#  This script demonstrates the blackbird command-line interface.
#  It creates a temporary local dataset, then walks through every
#  major CLI subcommand:
#
#    1.  reindex           — rebuild the dataset index
#    2.  stats             — show dataset statistics
#    3.  stats --missing   — show which tracks are missing a component
#    4.  find-tracks       — find tracks by component presence
#    5.  schema show       — display dataset schema
#    6.  location list     — list configured storage locations
#    7.  location add      — add a new storage location
#    8.  location remove   — remove a storage location
#    9.  clone             — clone from a remote WebDAV server
#   10.  sync              — incremental sync from a remote server
#   11.  resume            — resume an interrupted sync/clone
#
#  Steps 1-8 work fully offline with a local dummy dataset.
#  Steps 9-11 require a running WebDAV server and are shown
#  as reference commands (they will be skipped if no server is found).
# ==========================================================================

set -euo pipefail

# -- colours ---------------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m'

# -- configuration ---------------------------------------------------------
# WebDAV server URL (only used for remote examples)
WEBDAV_URL="${WEBDAV_URL:-webdav://localhost:8080}"

# Temporary directory — cleaned up on exit
WORK_DIR=$(mktemp -d /tmp/blackbird_cli_example.XXXXXX)
DATASET_DIR="$WORK_DIR/demo_dataset"
BACKUP_DIR="$WORK_DIR/backup_location"
CLONE_DIR="$WORK_DIR/cloned_dataset"

cleanup() {
    echo -e "\n${BLUE}Cleaning up ${WORK_DIR}...${NC}"
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# -- helpers ---------------------------------------------------------------
step() {
    echo -e "\n${BLUE}━━━ $1 ━━━${NC}"
}

ok() {
    echo -e "${GREEN}  OK${NC}"
}

skip() {
    echo -e "${YELLOW}  SKIPPED: $1${NC}"
}

# ==========================================================================
#  Build a dummy dataset on disk
# ==========================================================================
step "Creating dummy dataset at $DATASET_DIR"

mkdir -p "$DATASET_DIR/.blackbird"

# Schema file
cat > "$DATASET_DIR/.blackbird/schema.json" <<'SCHEMA'
{
  "version": "1.0",
  "components": {
    "instrumental.mp3": {
      "pattern": "*_instrumental.mp3",
      "multiple": false
    },
    "vocals_noreverb.mp3": {
      "pattern": "*_vocals_noreverb.mp3",
      "multiple": false
    },
    "mir.json": {
      "pattern": "*.mir.json",
      "multiple": false
    },
    "caption.txt": {
      "pattern": "*_caption.txt",
      "multiple": false
    }
  }
}
SCHEMA

# Locations file — Main points to the dataset root
cat > "$DATASET_DIR/.blackbird/locations.json" <<LOCATIONS
{"Main": "$DATASET_DIR"}
LOCATIONS

# Create artists / albums / tracks
create_track() {
    # Usage: create_track <dir> <base_name>
    local dir="$1" base="$2"
    echo -n "data" > "$dir/${base}_instrumental.mp3"
    echo -n "data" > "$dir/${base}_vocals_noreverb.mp3"
    echo -n "{}"   > "$dir/${base}.mir.json"
    echo -n "text" > "$dir/${base}_caption.txt"
}

# Artist_A — two albums
mkdir -p "$DATASET_DIR/Artist_A/Album_X [2020]"
create_track "$DATASET_DIR/Artist_A/Album_X [2020]" "01.Artist_A - Song_One"
create_track "$DATASET_DIR/Artist_A/Album_X [2020]" "02.Artist_A - Song_Two"
create_track "$DATASET_DIR/Artist_A/Album_X [2020]" "03.Artist_A - Song_Three"

mkdir -p "$DATASET_DIR/Artist_A/Album_Y [2022]"
create_track "$DATASET_DIR/Artist_A/Album_Y [2022]" "01.Artist_A - Song_Four"
create_track "$DATASET_DIR/Artist_A/Album_Y [2022]" "02.Artist_A - Song_Five"

# Artist_B — multi-CD album
mkdir -p "$DATASET_DIR/Artist_B/Album_Z [2021]/CD1"
create_track "$DATASET_DIR/Artist_B/Album_Z [2021]/CD1" "01.Artist_B - Intro"
create_track "$DATASET_DIR/Artist_B/Album_Z [2021]/CD1" "02.Artist_B - Main_Theme"

mkdir -p "$DATASET_DIR/Artist_B/Album_Z [2021]/CD2"
create_track "$DATASET_DIR/Artist_B/Album_Z [2021]/CD2" "01.Artist_B - Bonus"

# Artist_C — incomplete album (missing vocals on tracks 3-4)
mkdir -p "$DATASET_DIR/Artist_C/Album_W [2023]"
create_track "$DATASET_DIR/Artist_C/Album_W [2023]" "01.Artist_C - Alpha"
create_track "$DATASET_DIR/Artist_C/Album_W [2023]" "02.Artist_C - Beta"
# tracks 3-4 only have instrumental + mir (no vocals, no caption)
for base in "03.Artist_C - Gamma" "04.Artist_C - Delta"; do
    echo -n "data" > "$DATASET_DIR/Artist_C/Album_W [2023]/${base}_instrumental.mp3"
    echo -n "{}"   > "$DATASET_DIR/Artist_C/Album_W [2023]/${base}.mir.json"
done

echo "  Created 3 artists, 4 albums, 12 tracks"
ok

# ==========================================================================
#  1. Build index
# ==========================================================================
step "1. blackbird reindex — build the dataset index"
blackbird reindex "$DATASET_DIR"
ok

# ==========================================================================
#  2. Show statistics
# ==========================================================================
step "2. blackbird stats — show dataset statistics"
blackbird stats "$DATASET_DIR"
ok

# ==========================================================================
#  3. Show missing components
# ==========================================================================
step "3. blackbird stats --missing vocals_noreverb.mp3"
# Shows which tracks are missing the vocals component
blackbird stats "$DATASET_DIR" --missing vocals_noreverb.mp3
ok

# ==========================================================================
#  4. Find tracks by component presence
# ==========================================================================
step "4. blackbird find-tracks — find tracks with/without specific components"

# Find all tracks that have both instrumental and vocals
echo "  Tracks with instrumental.mp3 AND vocals_noreverb.mp3:"
blackbird find-tracks "$DATASET_DIR" --has instrumental.mp3,vocals_noreverb.mp3

# Find tracks missing vocals
echo ""
echo "  Tracks missing vocals_noreverb.mp3:"
blackbird find-tracks "$DATASET_DIR" --missing vocals_noreverb.mp3

# Find tracks by artist
echo ""
echo "  All tracks by Artist_B:"
blackbird find-tracks "$DATASET_DIR" --artist Artist_B
ok

# ==========================================================================
#  5. Schema show (local)
# ==========================================================================
step "5. blackbird schema show — display dataset schema"
blackbird schema show "$DATASET_DIR"
ok

# ==========================================================================
#  6. Location list
# ==========================================================================
step "6. blackbird location list — show configured locations"
blackbird location list "$DATASET_DIR"
ok

# ==========================================================================
#  7. Location add
# ==========================================================================
step "7. blackbird location add — add a backup location"

# Create backup location with some data
mkdir -p "$BACKUP_DIR/Artist_D/Album_V [2024]"
echo -n "data" > "$BACKUP_DIR/Artist_D/Album_V [2024]/01.Artist_D - Rain_instrumental.mp3"
echo -n "{}"   > "$BACKUP_DIR/Artist_D/Album_V [2024]/01.Artist_D - Rain.mir.json"

blackbird location add "$DATASET_DIR" Backup "$BACKUP_DIR"
echo "  Added Backup location"

# Show updated locations
blackbird location list "$DATASET_DIR"

# Rebuild to pick up new location
blackbird reindex "$DATASET_DIR"
echo "  After reindex with Backup location:"
blackbird stats "$DATASET_DIR"
ok

# ==========================================================================
#  8. Location remove
# ==========================================================================
step "8. blackbird location remove — remove the backup location"
# The --yes flag confirms removal without interactive prompt
blackbird location remove "$DATASET_DIR" Backup --yes
echo "  Removed Backup location"

blackbird location list "$DATASET_DIR"
blackbird reindex "$DATASET_DIR"
ok

# ==========================================================================
#  9-11. Remote operations (require a WebDAV server)
# ==========================================================================
step "9-11. Remote operations (clone / sync / resume)"

echo "  These commands require a running WebDAV server."
echo "  Set WEBDAV_URL env var to point to your server."
echo ""
echo "  Reference commands:"
echo ""
echo "  # Show remote dataset schema"
echo "  blackbird schema show \$WEBDAV_URL"
echo ""
echo "  # Clone specific components from specific artists"
echo "  blackbird clone \$WEBDAV_URL $CLONE_DIR \\"
echo "      --components instrumental.mp3,vocals_noreverb.mp3 \\"
echo "      --artists Artist_A,Artist_B"
echo ""
echo "  # Clone a proportion (e.g. 10%) of the dataset"
echo "  blackbird clone \$WEBDAV_URL $CLONE_DIR \\"
echo "      --components instrumental.mp3 \\"
echo "      --proportion 0.1"
echo ""
echo "  # Incremental sync — only download new/changed files"
echo "  blackbird sync \$WEBDAV_URL $CLONE_DIR \\"
echo "      --components instrumental.mp3,mir.json \\"
echo "      --parallel 4"
echo ""
echo "  # Sync only tracks missing a specific component"
echo "  blackbird sync \$WEBDAV_URL $CLONE_DIR \\"
echo "      --components vocals_noreverb.mp3 \\"
echo "      --missing instrumental.mp3"
echo ""
echo "  # Resume an interrupted clone or sync"
echo "  blackbird resume $CLONE_DIR"
echo ""
echo "  # Sync to a specific storage location"
echo "  blackbird sync \$WEBDAV_URL $CLONE_DIR \\"
echo "      --components mir.json \\"
echo "      --target-location Backup"

skip "No WebDAV server — showing reference commands only"

# ==========================================================================
#  Done
# ==========================================================================
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  All CLI examples completed successfully!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
