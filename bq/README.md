## BigQuery Integration

This folder contains schemas and scripts to load data from GCS into BigQuery, and a Cloud Function to automate loads on new files in GCS.

### Structure
- `schemas/` — optional table schema JSON files
- `scripts/load_bq_from_gcs.py` — run a BigQuery load job for a given GCS URI
- `cloud_functions/bq_loader/` — Cloud Function (Gen2) to trigger a load on GCS upload

### Quick Start: Manual Load
```bash
python bq/scripts/load_bq_from_gcs.py \
  --project YOUR_GCP_PROJECT \
  --dataset raw_layer \
  --table summary \
  --gcs-uri gs://your_bucket/exports/daily/export_20250915_172107.jsonl \
  --source-format NEWLINE_DELIMITED_JSON \
  --autodetect
```

### Deploy Cloud Function (Gen2)
```bash
gcloud functions deploy bq_loader \
  --gen2 \
  --runtime python312 \
  --region YOUR_REGION \
  --source bq/cloud_functions/bq_loader \
  --entry-point trigger_bigquery_load \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=your_bucket" \
  --set-env-vars "BQ_PROJECT=YOUR_GCP_PROJECT,BQ_DATASET=raw_layer,BQ_TABLE=summary,BQ_SOURCE_FORMAT=NEWLINE_DELIMITED_JSON,BQ_WRITE_DISPOSITION=WRITE_APPEND"
```

Ensure the function's service account has roles: BigQuery Data Editor, BigQuery Job User, and Storage Object Viewer.
