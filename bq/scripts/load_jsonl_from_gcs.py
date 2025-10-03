#!/usr/bin/env python3
import logging
import json
from pathlib import Path
from google.cloud import bigquery

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("bq_loader_script")

# Configure these values or set via environment when running
PROJECT_ID = "consummate-rig-466909-i6"
DATASET = "my_raw_dataset"
TABLE = "events2"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/exports/export_20251002_181352.jsonl"
WRITE_DISPOSITION = "WRITE_APPEND"  # or WRITE_TRUNCATE, WRITE_EMPTY
SCHEMA_PATH = Path(__file__).parents[2] / "schema" / "glamira_schema_raw.json"


def _to_bq_schema_fields(spec_list: list[dict]) -> list[bigquery.SchemaField]:
    fields: list[bigquery.SchemaField] = []
    for spec in spec_list:
        name = spec.get("name")
        field_type = (spec.get("type") or spec.get(
            "field_type") or "STRING").upper()
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


def load_jsonl_from_gcs() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=getattr(
            bigquery.WriteDisposition, WRITE_DISPOSITION),
        ignore_unknown_values=True,
        max_bad_records=100,
    )

    # Load schema from schema/glamira_schema_raw.json if present
    if SCHEMA_PATH.exists():
        LOGGER.info("Using schema file: %s", SCHEMA_PATH)
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_list = json.load(f)
        job_config.schema = _to_bq_schema_fields(schema_list)
    else:
        LOGGER.warning(
            "Schema file not found at %s; falling back to autodetect", SCHEMA_PATH)
        job_config.autodetect = True

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
