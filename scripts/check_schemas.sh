#!/bin/bash
# Verify that JSON schemas are up-to-date with Rust types.
#
# Use in CI or as a pre-commit check:
#   ./scripts/check_schemas.sh
#
# Exit code 0 = schemas are in sync
# Exit code 1 = schemas are outdated (regeneration needed)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SCHEMA_DIR="$ROOT_DIR/schemas"

echo "=== Checking schema sync ==="

# Save current schemas
TEMP_DIR=$(mktemp -d)
if [ -d "$SCHEMA_DIR" ]; then
    cp -r "$SCHEMA_DIR"/*.json "$TEMP_DIR/" 2>/dev/null || true
fi

# Regenerate schemas
cd "$ROOT_DIR"
cargo run -p tradersdk-schema-export --features json-schema -- "$SCHEMA_DIR" 2>/dev/null

# Compare
DIFF=$(diff -rq "$TEMP_DIR" "$SCHEMA_DIR" 2>/dev/null || true)

# Restore original schemas
if [ -d "$TEMP_DIR" ] && [ "$(ls -A "$TEMP_DIR")" ]; then
    cp -r "$TEMP_DIR"/*.json "$SCHEMA_DIR/"
fi
rm -rf "$TEMP_DIR"

if [ -n "$DIFF" ]; then
    echo "ERROR: JSON schemas are out of date!"
    echo ""
    echo "Differences found:"
    echo "$DIFF"
    echo ""
    echo "Run './scripts/generate_types.sh' to update."
    exit 1
fi

echo "OK: All schemas are up-to-date."
