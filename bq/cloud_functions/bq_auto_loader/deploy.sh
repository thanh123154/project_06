#!/bin/bash
# Deploy Cloud Function for BigQuery Auto Loader

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

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
cp "$ROOT_DIR/../schema/glamira_schema_raw.json" ./schema.json

# Deploy function
echo "Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
  --runtime python311 \
  --trigger-event-type google.storage.object.finalize \
  --trigger-resource "$BUCKET_NAME" \
  --trigger-event-filters path-pattern="exports/daily/*.jsonl" \
  --set-env-vars PROJECT_ID="$PROJECT_ID" \
  --set-env-vars DATASET="$DATASET" \
  --set-env-vars TABLE="$TABLE" \
  --set-env-vars WRITE_DISPOSITION=WRITE_APPEND \
  --set-env-vars MAX_BAD_RECORDS=1000 \
  --set-env-vars SCHEMA_PATH=/workspace/schema.json \
  --memory=1GB \
  --timeout=1800s \
  --max-instances=10 \
  --region=us-central1

echo "Cloud Function deployed successfully!"
echo "Function URL: https://console.cloud.google.com/functions/details/us-central1/$FUNCTION_NAME?project=$PROJECT_ID"

# Test with a sample file
echo "Testing with sample file..."
TEST_FILE="test_auto_load_$(date +%Y%m%d_%H%M%S).jsonl"
echo '{"test": "data", "timestamp": "'$(date -Iseconds)'"}' | gsutil cp - "gs://$BUCKET_NAME/exports/daily/$TEST_FILE"

echo "Test file uploaded: gs://$BUCKET_NAME/exports/daily/$TEST_FILE"
echo "Check logs: gcloud functions logs read $FUNCTION_NAME --limit=20"
