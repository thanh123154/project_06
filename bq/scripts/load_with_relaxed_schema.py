#!/usr/bin/env python3
"""
Load JSONL directly to BigQuery with relaxed schema to handle mixed data types.
This avoids downloading the large file to VM.
"""
import logging
import json
from pathlib import Path
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger("bq_direct_loader")

PROJECT_ID = "consummate-rig-466909-i6"
DATASET = "my_raw_dataset"
TABLE = "events2"
GCS_URI = "gs://first-bucket-practice-for-data-engineer/exports/daily/export_20250929_065110_clean.jsonl"
SCHEMA_PATH = Path(__file__).parents[2] / "schema" / "glamira_schema_raw.json"


def create_relaxed_schema():
    """Create a relaxed schema that handles mixed data types"""
    return [
        bigquery.SchemaField("time_stamp", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("ip", "STRING", "NULLABLE"),
        bigquery.SchemaField("user_agent", "STRING", "NULLABLE"),
        bigquery.SchemaField("resolution", "STRING", "NULLABLE"),
        bigquery.SchemaField("user_id_db", "STRING", "NULLABLE"),
        bigquery.SchemaField("device_id", "STRING", "NULLABLE"),
        bigquery.SchemaField("api_version", "STRING", "NULLABLE"),
        bigquery.SchemaField("store_id", "STRING", "NULLABLE"),
        bigquery.SchemaField("local_time", "STRING", "NULLABLE"),
        bigquery.SchemaField("show_recommendation", "STRING", "NULLABLE"),
        bigquery.SchemaField("collection", "STRING", "NULLABLE"),
        bigquery.SchemaField("product_id", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("session_id", "STRING", "NULLABLE"),
        bigquery.SchemaField("cart_products", "STRING",
                             "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField("recommendation_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField("recommendation_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField("recommendation_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField("recommendation_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField("recommendation_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_view_all_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_id", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_view_all_clicked_product_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_type", "STRING", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_view_all_clicked_product_recommendation_view_all_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_id", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_type", "STRING", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_clicked", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_visible", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_noticed", "BOOLEAN", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_position", "INTEGER", "NULLABLE"),
        bigquery.SchemaField(
            "recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_type", "STRING", "NULLABLE"),
        bigquery.SchemaField("recommendation_view_all_clicked_product_recommendation_view_all_clicked_product_recommendation_view_all_products",
                             "STRING", "NULLABLE"),  # Store as JSON string
    ]


def load_with_relaxed_schema():
    """Load JSONL with relaxed schema that handles mixed data types"""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace existing data
        ignore_unknown_values=True,
        max_bad_records=10000,  # Allow many bad records
        schema=create_relaxed_schema(),
    )

    LOGGER.info("Loading with relaxed schema (complex fields as STRING)...")
    LOGGER.info("Submitting load job: %s -> %s", GCS_URI, table_id)

    job = client.load_table_from_uri(
        GCS_URI,
        table_id,
        job_config=job_config,
    )

    LOGGER.info("Started job: %s", job.job_id)
    LOGGER.info(
        "Waiting for completion... (this may take 10-30 minutes for 35GB)")

    result = job.result()
    LOGGER.info("Job finished: %s", job.job_id)

    table = client.get_table(table_id)
    LOGGER.info("Loaded %d rows into %s", table.num_rows, table_id)

    # Show sample data
    LOGGER.info("Sample data from loaded table:")
    query = f"SELECT * FROM `{table_id}` LIMIT 3"
    query_job = client.query(query)
    for row in query_job:
        LOGGER.info("Row: %s", dict(row))


if __name__ == "__main__":
    try:
        load_with_relaxed_schema()
    except Exception as exc:
        LOGGER.error("Load failed: %s", exc)
        raise
