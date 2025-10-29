#!/bin/bash
# Export summary.bson from GCS bucket to summary.jsonl

echo "🔄 Converting summary.bson from GCS to summary.jsonl..."

python export_to_gcs.py \
  --convert-bson \
  --gcs-bucket-name "data-engineer-us-central1" \
  --gcs-bson-source "summary.bson" \
  --output-jsonl "summary.jsonl"

echo "✅ Conversion complete. Output: summary.jsonl"
