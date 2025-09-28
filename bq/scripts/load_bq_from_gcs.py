#!/usr/bin/env python3
import os
import sys
import argparse

try:
    from google.cloud import bigquery  # type: ignore
except Exception as exc:
    print("Please install google-cloud-bigquery: pip install google-cloud-bigquery", file=sys.stderr)
    raise


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Load a GCS object into a BigQuery table.")
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--gcs-uri", required=True,
                   help="gs://bucket/path/to/object or wildcard")

    p.add_argument(
        "--schema", help="Path to schema JSON file (list of {name,type,mode})")
    p.add_argument("--autodetect", action="store_true")

    p.add_argument("--source-format", default="NEWLINE_DELIMITED_JSON",
                   choices=[
                       "CSV",
                       "NEWLINE_DELIMITED_JSON",
                       "PARQUET",
                       "AVRO",
                       "ORC",
                   ])
    p.add_argument("--write-disposition", default="WRITE_APPEND",
                   choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"])
    p.add_argument("--field-delimiter", default=",")
    p.add_argument("--skip-leading-rows", type=int, default=0)
    p.add_argument("--quote", default='"')

    return p.parse_args()


def load_table_from_gcs(
    project: str,
    dataset: str,
    table: str,
    gcs_uri: str,
    source_format: str,
    write_disposition: str,
    autodetect: bool,
    schema_path: str | None,
    field_delimiter: str,
    skip_leading_rows: int,
    quote: str,
) -> None:
    client = bigquery.Client(project=project)

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = getattr(bigquery.SourceFormat, source_format)
    job_config.write_disposition = getattr(
        bigquery.WriteDisposition, write_disposition)

    if source_format == "CSV":
        job_config.field_delimiter = field_delimiter
        job_config.skip_leading_rows = skip_leading_rows
        job_config.quote_character = quote

    if schema_path:
        from google.cloud.bigquery import SchemaField
        import json
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_list = json.load(f)
        job_config.schema = [SchemaField(**f) for f in schema_list]
    else:
        job_config.autodetect = autodetect

    job = client.load_table_from_uri(
        source_uris=gcs_uri,
        destination=table_id,
        job_config=job_config,
    )

    print(f"Started load job: {job.job_id}")
    result = job.result()
    print(f"Load completed. Output rows: {result.output_rows}")


if __name__ == "__main__":
    args = parse_args()
    load_table_from_gcs(
        project=args.project,
        dataset=args.dataset,
        table=args.table,
        gcs_uri=args.gcs_uri,
        source_format=args.source_format,
        write_disposition=args.write_disposition,
        autodetect=args.autodetect,
        schema_path=args.schema,
        field_delimiter=args.field_delimiter,
        skip_leading_rows=args.skip_leading_rows,
        quote=args.quote,
    )
