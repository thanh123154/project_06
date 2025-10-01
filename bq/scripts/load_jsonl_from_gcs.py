#!/usr/bin/env python3
from exporter.logging_utils import get_logger
from google.cloud import bigquery
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


# Configure these values or set via environment when running
PROJECT_ID = "your_project_id"
DATASET = "raw"
TABLE = "events"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/raw/export_20250929_065110_clean.jsonl"
WRITE_DISPOSITION = "WRITE_APPEND"  # or WRITE_TRUNCATE, WRITE_EMPTY

LOGGER = get_logger("bq_loader_script")


def load_jsonl_from_gcs() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=getattr(
            bigquery.WriteDisposition, WRITE_DISPOSITION),
    )

    LOGGER.info("Submitting load job: %s -> %s", GCS_URI, table_id)
    job = client.load_table_from_uri(
        GCS_URI,
        table_id,
        job_config=job_config,
    )

    LOGGER.info("Started job: %s", job.job_id)
    result = job.result()  # Wait for completion
    LOGGER.info("Job finished: %s", job.job_id)

    table = client.get_table(table_id)
    LOGGER.info("Loaded %d rows into %s.", table.num_rows, table_id)


if __name__ == "__main__":
    try:
        load_jsonl_from_gcs()
    except Exception as exc:
        LOGGER.error("BigQuery load failed: %s", exc)
        raise
