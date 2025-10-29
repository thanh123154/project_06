#!/bin/bash
# Deploy Cloud Function for BigQuery Product Name Loader

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Go up 3 levels: bq_product_name_loader -> cloud_functions -> bq -> project_06
ROOT_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

echo "🚀 Deploying BigQuery Product Name Loader Cloud Function..."

# ======================
# Configuration
# ======================
PROJECT_ID="secure-wonder-475603-v6"
FUNCTION_NAME="bq-product-name-loader"
BUCKET_NAME="data-engineer-us-central1"
DATASET="my_raw_dataset"
TABLE="product_name"

# ======================
# Prepare
# ======================
cd "$SCRIPT_DIR"

echo "📂 Copying schema file..."
cp "$ROOT_DIR/schema/product_name.json" ./schema.json

# ======================
# IAM Setup
# ======================
echo "🔑 Setting up IAM permissions..."
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/cloudbuild.builds.builder" \
  --quiet || echo "IAM permissions already set"

# ======================
# Deploy Cloud Function
# ======================
echo "☁️ Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime python311 \
  --entry-point bq_product_name_loader \
  --trigger-event-filters type=google.cloud.storage.object.v1.finalized \
  --trigger-event-filters bucket="$BUCKET_NAME" \
  --set-env-vars PROJECT_ID="$PROJECT_ID" \
  --set-env-vars DATASET="$DATASET" \
  --set-env-vars TABLE="$TABLE" \
  --set-env-vars WRITE_DISPOSITION=WRITE_APPEND \
  --set-env-vars MAX_BAD_RECORDS=1000 \
  --set-env-vars SCHEMA_PATH=/workspace/schema.json \
  --memory=1Gi \
  --timeout=540s \
  --max-instances=10 \
  --region=us-central1

echo "✅ Cloud Function deployed successfully!"
echo "🌐 Function URL: https://console.cloud.google.com/functions/details/us-central1/$FUNCTION_NAME?project=$PROJECT_ID"

# ======================
# Test Upload
# ======================
echo "🧪 Testing with sample file..."
TEST_FILE="test_product_name_$(date +%Y%m%d_%H%M%S).jsonl"
TIMESTAMP=$(date -Iseconds)
echo "{\"product_id\": \"SKU001\", \"fetched_at\": 0}" | gsutil cp - "gs://$BUCKET_NAME/exports/product_name/$TEST_FILE"

echo "📤 Test file uploaded: gs://$BUCKET_NAME/exports/product_name/$TEST_FILE"
echo "🪵 Check logs: gcloud functions logs read $FUNCTION_NAME --limit=20"


