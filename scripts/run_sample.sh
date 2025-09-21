#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

: "${MONGO_URI:=mongodb://localhost:27017}"
: "${DB_NAME:=glamira}"
: "${COLLECTION_NAME:=summary}"

python export_to_gcs.py \
  --mongo-uri "$MONGO_URI" \
  --db "$DB_NAME" \
  --collection "$COLLECTION_NAME" \
  --query-file sample_query.json \
  --format jsonl \
  --batch-size 2000 \
  --flatten \
  --dry-run

echo "Done. Check the exports directory." 