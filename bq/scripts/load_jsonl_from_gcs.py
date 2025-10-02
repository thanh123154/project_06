#!/usr/bin/env python3
import logging
import json
from pathlib import Path
from urllib.parse import urlparse
from google.cloud import bigquery
from google.cloud import storage

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("bq_loader_script")

# Configure these values or set via environment when running
PROJECT_ID = "consummate-rig-466909-i6"
DATASET = "my_raw_dataset"
TABLE = "events2"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/exports/daily/export_20250929_065110_clean.jsonl"
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


def _ensure_array(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _gcs_uri_parts(uri: str) -> tuple[str, str]:
    o = urlparse(uri)
    if o.scheme != "gs":
        raise ValueError("GCS URI must start with gs://")
    bucket = o.netloc
    blob_name = o.path.lstrip("/")
    return bucket, blob_name


def _fix_jsonl_gcs(source_gcs_uri: str) -> str:
    bucket_name, blob_name = _gcs_uri_parts(source_gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    src_blob = bucket.blob(blob_name)

    fixed_blob_name = blob_name.rsplit(".", 1)[0] + "_fixed.jsonl"
    dst_blob = bucket.blob(fixed_blob_name)

    LOGGER.info("Downloading %s for normalization...", source_gcs_uri)
    raw_bytes = src_blob.download_as_bytes()
    lines = raw_bytes.decode("utf-8", errors="ignore").splitlines()

    LOGGER.info("Normalizing cart_products.option to arrays...")
    out_lines: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            # Skip invalid lines silently; BigQuery will tolerate some with max_bad_records
            continue
        cps = obj.get("cart_products")
        if isinstance(cps, list):
            for it in cps:
                if isinstance(it, dict) and "option" in it:
                    it["option"] = _ensure_array(it.get("option"))
        elif isinstance(cps, dict):
            if "option" in cps:
                cps["option"] = _ensure_array(cps.get("option"))
            obj["cart_products"] = [cps]
        out_lines.append(json.dumps(obj, ensure_ascii=False))

    fixed_payload = ("\n".join(out_lines) + "\n").encode("utf-8")
    LOGGER.info("Uploading normalized file to gs://%s/%s",
                bucket_name, fixed_blob_name)
    dst_blob.upload_from_string(fixed_payload, content_type="application/json")

    return f"gs://{bucket_name}/{fixed_blob_name}"


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

    # Skip normalization for now - use original file
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
