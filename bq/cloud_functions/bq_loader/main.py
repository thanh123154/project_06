import os
import json
from typing import Any, Dict

from google.cloud import bigquery


def _make_table_id() -> str:
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    table = os.environ["BQ_TABLE"]
    return f"{project}.{dataset}.{table}"


def bq_auto_loader(event: Dict[str, Any], context: Any) -> None:
    # Event structure for Storage (finalized) - CloudEvents-like dict in gen2
    bucket = event.get("bucket") or event.get("data", {}).get("bucket")
    name = event.get("name") or event.get("data", {}).get("name")
    if not bucket or not name:
        print("Missing bucket/name in event; skipping")
        return

    gcs_uri = f"gs://{bucket}/{name}"

    project = os.environ.get("BQ_PROJECT")
    if not project:
        raise RuntimeError("BQ_PROJECT env var is required")

    client = bigquery.Client(project=project)
    table_id = _make_table_id()

    source_format = os.environ.get(
        "BQ_SOURCE_FORMAT", "NEWLINE_DELIMITED_JSON")
    write_disposition = os.environ.get("BQ_WRITE_DISPOSITION", "WRITE_APPEND")
    autodetect = os.environ.get("BQ_AUTODETECT", "true").lower() == "true"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = getattr(bigquery.SourceFormat, source_format)
    job_config.write_disposition = getattr(
        bigquery.WriteDisposition, write_disposition)
    job_config.autodetect = autodetect

    print(f"Starting load: {gcs_uri} -> {table_id}")
    job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()  # Wait for completion

    print(f"Load completed. Job ID: {job.job_id}")
