#!/bin/bash
# Deploy Cloud Function for BigQuery Auto Loader

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Go up 3 levels: bq_auto_loader -> cloud_functions -> bq -> project_06
ROOT_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

echo "Deploying BigQuery Auto Loader Cloud Function..."

# Configuration
PROJECT_ID="secure-wonder-475603-v6"
FUNCTION_NAME="bq-auto-loader"
BUCKET_NAME="data-engineer-us-central1"
DATASET="my_raw_dataset"
TABLE="events2"

# Change to function directory
cd "$SCRIPT_DIR"

# Copy schema file
echo "Copying schema file..."
cp "$ROOT_DIR/schema/glamira_schema_raw.json" ./schema.json

# Deploy function
echo "Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime python311 \
  --entry-point bq_auto_loader \
  --trigger-event-filters type=google.cloud.storage.object.v1.finalized \
  --trigger-event-filters bucket="$BUCKET_NAME" \
  --trigger-event-filters objectPrefix="exports/daily/" \
  --set-env-vars PROJECT_ID="$PROJECT_ID" \
  --set-env-vars DATASET="$DATASET" \
  --set-env-vars TABLE="$TABLE" \
  --set-env-vars WRITE_DISPOSITION=WRITE_APPEND \
  --set-env-vars MAX_BAD_RECORDS=1000 \
  --set-env-vars SCHEMA_PATH=/workspace/schema.json \
  --memory=1Gi \
  --timeout=1800s \
  --max-instances=10 \
  --region=us-central1

echo "Cloud Function deployed successfully!"
echo "Function URL: https://console.cloud.google.com/functions/details/us-central1/$FUNCTION_NAME?project=$PROJECT_ID"

# Test with a sample file
echo "Testing with sample file..."
TEST_FILE="test_auto_load_$(date +%Y%m%d_%H%M%S).jsonl"
TIMESTAMP=$(date -Iseconds)
echo "{\"test\": \"data\", \"timestamp\": \"$TIMESTAMP\"}" | gsutil cp - "gs://$BUCKET_NAME/exports/daily/$TEST_FILE"

echo "Test file uploaded: gs://$BUCKET_NAME/exports/daily/$TEST_FILE"
echo "Check logs: gcloud functions logs read $FUNCTION_NAME --limit=20"
