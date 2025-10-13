#!/usr/bin/env python3
"""
Cloud Function to automatically load new JSONL files from GCS to BigQuery.
Triggered by GCS object finalize events.
"""
import json
import logging
import os
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("bq_auto_loader")

# Configuration from environment variables
PROJECT_ID = os.getenv("PROJECT_ID", "consummate-rig-466909-i6")
DATASET = os.getenv("DATASET", "my_raw_dataset")
TABLE = os.getenv("TABLE", "events2")
WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION", "WRITE_APPEND")
MAX_BAD_RECORDS = int(os.getenv("MAX_BAD_RECORDS", "1000"))
IGNORE_UNKNOWN_VALUES = os.getenv(
    "IGNORE_UNKNOWN_VALUES", "true").lower() == "true"

# Schema configuration
SCHEMA_PATH = os.getenv(
    "SCHEMA_PATH", "/workspace/schema/glamira_schema_raw.json")

# File patterns to process
ALLOWED_EXTENSIONS = {".jsonl", ".json"}
EXCLUDED_PATTERNS = {"_fixed.jsonl", "_chunk_", "_temp"}


def _to_bq_schema_fields(spec_list: list) -> list[bigquery.SchemaField]:
    """Convert JSON schema to BigQuery SchemaField objects"""
    fields = []
    for spec in spec_list:
        name = spec.get("name")
        field_type = (spec.get("type") or "STRING").upper()
        mode = (spec.get("mode") or "NULLABLE").upper()
        description = spec.get("description")
        sub_specs = spec.get("fields") or []
        sub_fields = _to_bq_schema_fields(sub_specs) if sub_specs else None
        fields.append(
            bigquery.SchemaField(
                name=name,
                field_type=field_type,
                mode=mode,
                description=description,
                fields=sub_fields,
            )
        )
    return fields


def _load_schema() -> list[bigquery.SchemaField] | None:
    """Load schema from file if available"""
    if not os.path.exists(SCHEMA_PATH):
        LOGGER.warning("Schema file not found at %s", SCHEMA_PATH)
        return None

    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_list = json.load(f)
        return _to_bq_schema_fields(schema_list)
    except Exception as exc:
        LOGGER.error("Failed to load schema: %s", exc)
        return None


def _should_process_file(file_name: str) -> bool:
    """Check if file should be processed based on name patterns"""
    # Check extension
    if not any(file_name.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return False

    # Check excluded patterns
    if any(pattern in file_name for pattern in EXCLUDED_PATTERNS):
        LOGGER.info("Skipping file with excluded pattern: %s", file_name)
        return False

    return True


def _get_file_size_gcs(bucket_name: str, blob_name: str) -> int:
    """Get file size from GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.reload()
        return blob.size or 0
    except Exception as exc:
        LOGGER.warning("Failed to get file size: %s", exc)
        return 0


def trigger_bigquery_load(event, context):
    """
    Legacy function name for backward compatibility.
    Calls bq_auto_loader function.
    """
    return bq_auto_loader(event, context)


def bq_auto_loader(event, context):
    """
    Cloud Function entry point triggered by GCS object finalize events.

    Args:
        event: GCS event data
        context: Cloud Function context
    """
    LOGGER.info("Cloud Function triggered by GCS event")

    # Extract file information from event
    try:
        bucket_name = event["bucket"]
        file_name = event["name"]
        gcs_uri = f"gs://{bucket_name}/{file_name}"

        LOGGER.info("Processing file: %s", gcs_uri)

        # Check if file should be processed
        if not _should_process_file(file_name):
            LOGGER.info("Skipping file: %s", file_name)
            return {"status": "skipped", "reason": "file_pattern"}

        # Get file size for logging
        file_size = _get_file_size_gcs(bucket_name, file_name)
        LOGGER.info("File size: %d bytes (%.2f MB)",
                    file_size, file_size / (1024 * 1024))

    except Exception as exc:
        LOGGER.error("Failed to parse event data: %s", exc)
        return {"status": "error", "reason": "event_parsing", "error": str(exc)}

    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

        LOGGER.info("Target table: %s", table_id)

    except Exception as exc:
        LOGGER.error("Failed to initialize BigQuery client: %s", exc)
        return {"status": "error", "reason": "bq_client", "error": str(exc)}

    # Configure load job
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=getattr(
                bigquery.WriteDisposition, WRITE_DISPOSITION),
            ignore_unknown_values=IGNORE_UNKNOWN_VALUES,
            max_bad_records=MAX_BAD_RECORDS,
        )

        # Load schema if available
        schema = _load_schema()
        if schema:
            job_config.schema = schema
            LOGGER.info("Using predefined schema with %d fields", len(schema))
        else:
            job_config.autodetect = True
            LOGGER.info("Using schema autodetect")

    except Exception as exc:
        LOGGER.error("Failed to configure load job: %s", exc)
        return {"status": "error", "reason": "job_config", "error": str(exc)}

    # Start BigQuery load job
    try:
        LOGGER.info("Starting BigQuery load job...")
        job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
        )

        LOGGER.info("Load job started: %s", job.job_id)

        # Wait for completion (with timeout for large files)
        timeout_seconds = 1800  # 30 minutes
        result = job.result(timeout=timeout_seconds)

        LOGGER.info("Load job completed: %s", job.job_id)

        # Get final table info
        table = client.get_table(table_id)
        rows_loaded = table.num_rows

        LOGGER.info(
            "Successfully loaded data. Total rows in table: %d", rows_loaded)

        return {
            "status": "success",
            "job_id": job.job_id,
            "gcs_uri": gcs_uri,
            "table_id": table_id,
            "rows_loaded": rows_loaded,
            "file_size_bytes": file_size,
        }

    except Exception as exc:
        LOGGER.error("BigQuery load job failed: %s", exc)
        return {
            "status": "error",
            "reason": "load_job",
            "error": str(exc),
            "gcs_uri": gcs_uri,
            "table_id": table_id,
        }


# For local testing
if __name__ == "__main__":
    # Test event structure
    test_event = {
        "bucket": "first-bucket-practice-for-data-engineer",
        "name": "exports/daily/export_20250929_065110_clean.jsonl",
        "contentType": "application/jsonl",
        "size": "35463362574",
        "timeCreated": "2025-09-29T06:51:10.000Z",
        "updated": "2025-09-29T06:51:10.000Z"
    }

    test_context = type('Context', (), {
        'event_id': 'test-event-id',
        'timestamp': '2025-09-29T06:51:10.000Z',
        'event_type': 'google.storage.object.finalize'
    })()

    print("Testing Cloud Function locally...")
    result = bq_auto_loader(test_event, test_context)
    print("Result:", json.dumps(result, indent=2))
