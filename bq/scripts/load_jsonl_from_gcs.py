#!/usr/bin/env python3
import logging
from google.cloud import bigquery

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("bq_loader_script")


# Configure these values or set via environment when running
PROJECT_ID = "consummate-rig-466909-i6"
DATASET = "my_raw_dataset"
TABLE = "events"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/exports/export_20250929_085503.parquet"
WRITE_DISPOSITION = "WRITE_APPEND"  # or WRITE_TRUNCATE, WRITE_EMPTY


def load_jsonl_from_gcs() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
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
