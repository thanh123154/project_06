## Mongo Export to GCS

Export MongoDB collections in batches to CSV/JSONL/Parquet/Avro/ORC and optionally upload to Google Cloud Storage.

### Setup
1. Create and activate a virtualenv (optional).
2. Install deps:
```bash
pip install -r requirements.txt
```
3. Configure environment (copy and edit):
```bash
cp env.example .env
```
If using GCS upload, set `GOOGLE_APPLICATION_CREDENTIALS` to your service account JSON.

### Usage
Basic dry-run (writes locally only):
```bash
python export_to_gcs.py \
  --mongo-uri "$MONGO_URI" \
  --db glamira \
  --collection summary \
  --query-file sample_query.json \
  --format jsonl \
  --batch-size 5000 \
  --flatten \
  --dry-run
```

Upload to GCS after local export:
```bash
python export_to_gcs.py \
  --db "$DB_NAME" \
  --collection "$COLLECTION_NAME" \
  --query-json '{"collection": {"$in": ["view_product_detail"]}}' \
  --format parquet \
  --gcs-bucket "$GCS_BUCKET" \
  --gcs-prefix exports/daily
```

### CLI Options
- `--mongo-uri` (env `MONGO_URI`)
- `--db` (env `DB_NAME`)
- `--collection` (env `COLLECTION_NAME`)
- `--query-json` or `--query-file`
- `--fields` (projection and CSV header)
- `--batch-size` (env `BATCH_SIZE`, default 5000)
- `--format` csv|jsonl|parquet|avro|orc (env `EXPORT_FORMAT`)
- `--local-output-dir` (env `LOCAL_OUTPUT_DIR`)
- `--file-prefix` (env `FILE_PREFIX`)
- `--flatten` (flatten nested docs for tabular formats)
- `--gcs-bucket` and `--gcs-prefix`
- `--no-gcs-upload` (skip upload even if bucket provided)
- `--dry-run`

### Notes
- Logs are written to `logs/export.log` and console.
- Non-tabular types are coerced to strings for Parquet/Avro/ORC to ensure schema stability.
- Ensure Mongo has appropriate indexes for your query to avoid long scans. 