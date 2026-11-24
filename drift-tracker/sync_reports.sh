#!/bin/bash
# Copy drift reports from production to the drift-tracker tool on Toolforge.
# Uses the ssh config of this machine for both hosts.
set -euo pipefail

SRC_HOST=${SRC_HOST:-deploy2003.codfw.wmnet}
SRC_DIR=${SRC_DIR:-db-analyzor-tools}
DEST_HOST=${DEST_HOST:-login.toolforge.org}
DEST_DIR=${DEST_DIR:-/data/project/drift-tracker/www/python/src/drifts}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

scp "$SRC_HOST:$SRC_DIR/drifts_*.json" "$tmp/"
ssh "$DEST_HOST" "mkdir -p '$DEST_DIR'"
sftp -b - "$DEST_HOST" <<EOF
put $tmp/drifts_*.json $DEST_DIR/
chmod 664 $DEST_DIR/drifts_*.json
EOF
echo "Copied $(ls "$tmp" | wc -l) reports to $DEST_HOST:$DEST_DIR"
