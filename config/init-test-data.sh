#!/bin/sh
# ============================================================================
# Initialize test data in the storeBase volume for fslink mode.
# Creates the same test structure as the pfcon req_resp_flow.md example.
# ============================================================================

set -e

STOREBASE="/var/local/storeBase"

echo "Creating test data in $STOREBASE..."

# Input directory with a test file
mkdir -p "$STOREBASE/home/user/cube/test"
echo "This is a test file for pl-simpledsapp." > "$STOREBASE/home/user/cube/test/test_file.txt"
echo "A second test line." >> "$STOREBASE/home/user/cube/test/test_file.txt"

# .chrislink pointing to the test folder
echo "home/user/cube/test" > "$STOREBASE/home/user/cube/test.chrislink"

# Output directory (plugin writes directly here in fslink mode)
mkdir -p "$STOREBASE/home/user/cube_out"

# Make everything writable
chmod -R 777 "$STOREBASE/home"

echo "Test data created:"
find "$STOREBASE/home" -type f | head -20
echo "Done."
