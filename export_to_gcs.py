#!/usr/bin/env python3
import os
import sys
import argparse

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from exporter import (
    get_logger,
    load_query,
    export,
    upload_to_gcs,
)
from exporter.mongo_exporter import get_mongo_client  # reuse client for counting

LOGGER = get_logger("export_cli")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export MongoDB data in batches and optionally upload to GCS.")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI",
                   "mongodb://localhost:27017"))
    p.add_argument("--db", required=False, default=os.getenv("DB_NAME"))
    p.add_argument("--collection", required=False,
                   default=os.getenv("COLLECTION_NAME"))

    group_q = p.add_mutually_exclusive_group()
    group_q.add_argument(
        "--query-json", help="Inline JSON string for Mongo query")
    group_q.add_argument(
        "--query-file", help="Path to JSON file with Mongo query")

    p.add_argument("--fields", nargs="*",
                   help="Optional projection fields for output/CSV header")
    p.add_argument("--batch-size", type=int,
                   default=int(os.getenv("BATCH_SIZE", "5000")))
    p.add_argument("--format", choices=["csv", "jsonl", "parquet",
                   "avro", "orc"], default=os.getenv("EXPORT_FORMAT", "jsonl"))
    p.add_argument("--local-output-dir", default=os.getenv("LOCAL_OUTPUT_DIR",
                   os.path.join(os.getcwd(), "exports")))
    p.add_argument("--file-prefix", default=os.getenv("FILE_PREFIX", "export"))

    p.add_argument("--flatten", action="store_true",
                   help="Flatten nested documents for tabular formats")

    # Verification
    p.add_argument("--verify-count", action="store_true",
                   help="Compare Mongo count_documents with exported record count")

    # GCS options
    p.add_argument("--gcs-bucket", default=os.getenv("GCS_BUCKET"))
    p.add_argument("--gcs-prefix", default=os.getenv("GCS_PREFIX", ""))
    p.add_argument("--no-gcs-upload", action="store_true",
                   help="Skip GCS upload even if bucket is provided")

    p.add_argument("--dry-run", action="store_true",
                   help="Run without uploading to GCS")

    return p


def count_exported_records(local_path: str, fmt: str) -> int:
    fmt = fmt.lower()
    if fmt == "jsonl":
        with open(local_path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    if fmt == "csv":
        with open(local_path, "r", encoding="utf-8") as f:
            total = sum(1 for _ in f)
            return max(0, total - 1)  # minus header
    if fmt == "parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore
        except Exception as exc:
            LOGGER.warning(
                f"Cannot count parquet rows (pyarrow missing): {exc}")
            return -1
        pf = pq.ParquetFile(local_path)
        return pf.metadata.num_rows if pf.metadata is not None else -1
    # Unsupported precise counting for avro/orc here
    LOGGER.warning(f"Record counting not implemented for format: {fmt}")
    return -1


def verify_counts(mongo_uri: str, db: str, collection: str, query: dict, exported_path: str, fmt: str) -> None:
    try:
        client = get_mongo_client(mongo_uri)
        expected = client[db][collection].count_documents(query)
    except Exception as exc:
        LOGGER.error(f"Failed to count expected documents in Mongo: {exc}")
        return
    actual = count_exported_records(exported_path, fmt)
    if actual < 0:
        LOGGER.info(
            f"Expected (Mongo): {expected}. Actual (file): unknown for format {fmt}.")
        return
    status = "MATCH" if expected == actual else "MISMATCH"
    LOGGER.info(
        f"Verification: expected={expected}, actual={actual} -> {status}")


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    parser = build_arg_parser()
    args = parser.parse_args()

    missing = []
    if not args.db:
        missing.append("--db or DB_NAME")
    if not args.collection:
        missing.append("--collection or COLLECTION_NAME")
    if missing:
        parser.error("Missing required arguments: " + ", ".join(missing))

    query = load_query(args.query_json, args.query_file)

    try:
        local_path = export(
            mongo_uri=args.mongo_uri,
            db_name=args.db,
            collection_name=args.collection,
            query=query,
            fields=args.fields,
            batch_size=args.batch_size,
            fmt=args.format,
            local_output_dir=args.local_output_dir,
            file_prefix=args.file_prefix,
            flatten_for_tabular=args.flatten or (
                args.format in ("csv", "orc")),
        )
    except Exception as exc:
        LOGGER.error(f"Export failed: {exc}")
        sys.exit(2)

    if args.verify_count:
        verify_counts(args.mongo_uri, args.db, args.collection,
                      query, local_path, args.format)

    if args.dry_run or args.no_gcs_upload or not args.gcs_bucket:
        LOGGER.info("Skipping GCS upload (dry-run/no-gcs/bucket not set)")
        print(local_path)
        return

    base_name = os.path.basename(local_path)
    gcs_dest = f"{args.gcs_prefix.rstrip('/') + '/' if args.gcs_prefix else ''}{base_name}"

    try:
        uri = upload_to_gcs(local_path, args.gcs_bucket, gcs_dest)
        print(uri)
    except Exception as exc:
        LOGGER.error(f"GCS upload failed: {exc}")
        sys.exit(3)


if __name__ == "__main__":
    main()
