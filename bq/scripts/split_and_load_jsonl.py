#!/usr/bin/env python3
"""
Script to split large JSONL file into smaller chunks for BigQuery loading.
This avoids timeout issues when normalizing large files.
"""
import logging
import json
import os
from pathlib import Path
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("jsonl_splitter")

PROJECT_ID = "consummate-rig-466909-i6"
DATASET = "my_raw_dataset"
TABLE = "events2"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/exports/daily/export_20250929_065110_clean.jsonl"
SCHEMA_PATH = Path(__file__).parents[2] / "schema" / "glamira_schema_raw.json"
CHUNK_SIZE_MB = 100  # Split into ~100MB chunks


def _ensure_array(value):
    """Ensure value is always an array"""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_cart_products(obj):
    """Normalize cart_products.option to always be arrays"""
    cps = obj.get("cart_products")
    if isinstance(cps, list):
        for item in cps:
            if isinstance(item, dict) and "option" in item:
                item["option"] = _ensure_array(item.get("option"))
    elif isinstance(cps, dict):
        if "option" in cps:
            cps["option"] = _ensure_array(cps.get("option"))
        obj["cart_products"] = [cps]  # Ensure cart_products is array
    return obj


def split_and_normalize_jsonl():
    """Split large JSONL file into smaller normalized chunks"""
    storage_client = storage.Client()
    bucket_name, blob_name = GCS_URI.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)

    LOGGER.info("Downloading %s for splitting...", GCS_URI)
    raw_bytes = source_blob.download_as_bytes()
    lines = raw_bytes.decode("utf-8", errors="ignore").splitlines()

    LOGGER.info("Processing %d lines...", len(lines))

    chunk_files = []
    current_chunk = []
    current_size = 0
    chunk_num = 0

    for line in lines:
        if not line.strip():
            continue

        try:
            obj = json.loads(line)
            normalized_obj = _normalize_cart_products(obj)
            normalized_line = json.dumps(normalized_obj, ensure_ascii=False)

            current_chunk.append(normalized_line)
            current_size += len(normalized_line.encode('utf-8'))

            # Create chunk when size limit reached
            if current_size > CHUNK_SIZE_MB * 1024 * 1024:  # Convert MB to bytes
                chunk_num += 1
                chunk_blob_name = f"{blob_name.rsplit('.', 1)[0]}_chunk_{chunk_num:03d}.jsonl"
                chunk_blob = bucket.blob(chunk_blob_name)

                chunk_content = "\n".join(current_chunk).encode('utf-8')
                chunk_blob.upload_from_string(
                    chunk_content, content_type="application/jsonl")

                chunk_gcs_uri = f"gs://{bucket_name}/{chunk_blob_name}"
                chunk_files.append(chunk_gcs_uri)
                LOGGER.info("Created chunk %d: %s (%d lines)",
                            chunk_num, chunk_gcs_uri, len(current_chunk))

                current_chunk = []
                current_size = 0

        except Exception as e:
            LOGGER.warning("Skipping invalid line: %s", e)
            continue

    # Upload final chunk
    if current_chunk:
        chunk_num += 1
        chunk_blob_name = f"{blob_name.rsplit('.', 1)[0]}_chunk_{chunk_num:03d}.jsonl"
        chunk_blob = bucket.blob(chunk_blob_name)

        chunk_content = "\n".join(current_chunk).encode('utf-8')
        chunk_blob.upload_from_string(
            chunk_content, content_type="application/jsonl")

        chunk_gcs_uri = f"gs://{bucket_name}/{chunk_blob_name}"
        chunk_files.append(chunk_gcs_uri)
        LOGGER.info("Created final chunk %d: %s (%d lines)",
                    chunk_num, chunk_gcs_uri, len(current_chunk))

    return chunk_files


def load_chunks_to_bigquery(chunk_files):
    """Load all chunks to BigQuery using wildcard pattern"""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Use wildcard pattern to load all chunks
    base_uri = GCS_URI.rsplit('/', 1)[0]
    wildcard_uri = f"{base_uri}/export_20250929_065110_clean_chunk_*.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        max_bad_records=1000,
    )

    # Load schema if available
    if SCHEMA_PATH.exists():
        LOGGER.info("Using schema file: %s", SCHEMA_PATH)
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_list = json.load(f)

        def _to_bq_schema_fields(spec_list):
            fields = []
            for spec in spec_list:
                name = spec.get("name")
                field_type = (spec.get("type") or "STRING").upper()
                mode = (spec.get("mode") or "NULLABLE").upper()
                description = spec.get("description")
                sub_specs = spec.get("fields") or []
                sub_fields = _to_bq_schema_fields(
                    sub_specs) if sub_specs else None
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

        job_config.schema = _to_bq_schema_fields(schema_list)
    else:
        LOGGER.warning("Schema file not found, using autodetect")
        job_config.autodetect = True

    LOGGER.info("Loading chunks with wildcard: %s", wildcard_uri)
    job = client.load_table_from_uri(
        wildcard_uri,
        table_id,
        job_config=job_config,
    )

    LOGGER.info("Started job: %s", job.job_id)
    result = job.result()
    LOGGER.info("Job finished: %s", job.job_id)

    table = client.get_table(table_id)
    LOGGER.info("Loaded %d rows into %s", table.num_rows, table_id)


if __name__ == "__main__":
    try:
        LOGGER.info("Starting JSONL split and normalize process...")
        chunk_files = split_and_normalize_jsonl()
        LOGGER.info("Created %d chunks", len(chunk_files))

        LOGGER.info("Loading chunks to BigQuery...")
        load_chunks_to_bigquery(chunk_files)

        LOGGER.info("Process completed successfully!")

    except Exception as exc:
        LOGGER.error("Process failed: %s", exc)
        raise
