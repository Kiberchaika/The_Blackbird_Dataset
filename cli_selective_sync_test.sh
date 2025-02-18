#!/bin/bash

# Colors for better output readability
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

# Test configuration
WEBDAV_URL="webdav://localhost:8080"
LOCAL_TEST_DIR="/tmp/blackbird_test_clone"
SELECTED_ARTISTS="Юта,7Б,19_84"  # Updated artist name
COMPONENTS="instrumental_audio,vocals_audio"  # Updated component names

echo -e "${BLUE}=== Blackbird Dataset Selective Clone Test ===${NC}\n"

# Function to print step headers
print_step() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to check command success
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Success${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
        exit 1
    fi
}

# Clean up any previous test directory
print_step "Cleaning up previous test directory"
rm -rf "$LOCAL_TEST_DIR"
mkdir -p "$LOCAL_TEST_DIR"
check_success

# Check if blackbird CLI is installed
print_step "Checking blackbird CLI installation"
which blackbird
check_success

# Show remote schema
print_step "Retrieving remote schema"
blackbird schema show "$WEBDAV_URL"
check_success

# Perform selective clone
print_step "Performing selective clone"
echo "Cloning from: $WEBDAV_URL"
echo "Selected artists: $SELECTED_ARTISTS"
echo "Selected components: $COMPONENTS"
echo ""

blackbird clone "$WEBDAV_URL" "$LOCAL_TEST_DIR" \
    --artists "$SELECTED_ARTISTS" \
    --components "$COMPONENTS"
check_success

# Verify cloned data
print_step "Verifying cloned data"
echo "Checking cloned dataset at: $LOCAL_TEST_DIR"
blackbird reindex "$LOCAL_TEST_DIR"
blackbird stats "$LOCAL_TEST_DIR"
check_success

echo -e "\n${GREEN}All tests completed successfully!${NC}" 