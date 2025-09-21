import os
import time
from typing import Any, Dict, List, Optional

import pymongo

from .logging_utils import get_logger
from .utils import sanitize_document, flatten_dict
from .writers import BaseBatchWriter, make_writer

LOGGER = get_logger("mongo_exporter")


def get_mongo_client(uri: str) -> pymongo.MongoClient:
    return pymongo.MongoClient(uri, serverSelectionTimeoutMS=10_000)


def export(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    query: Dict[str, Any],
    fields: Optional[List[str]],
    batch_size: int,
    fmt: str,
    local_output_dir: str,
    file_prefix: str,
    flatten_for_tabular: bool,
) -> str:
    client = get_mongo_client(mongo_uri)
    try:
        client.admin.command("ping")
        LOGGER.info("Connected to MongoDB")
    except Exception as exc:
        LOGGER.error(f"Cannot connect to MongoDB: {exc}")
        raise

    collection = client[db_name][collection_name]
    projection = None
    if fields:
        projection = {f: 1 for f in fields}

    os.makedirs(local_output_dir, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    ext = fmt.lower()
    local_path = os.path.join(
        local_output_dir, f"{file_prefix}_{timestamp}.{ext}")

    total_docs = 0
    writer: BaseBatchWriter = make_writer(
        fmt, local_path, header_fields=fields if fmt == "csv" else None)

    try:
        cursor = collection.find(
            query, projection=projection, no_cursor_timeout=True, batch_size=batch_size)
        with cursor:
            batch: List[Dict[str, Any]] = []
            for doc in cursor:
                batch.append(doc)
                if len(batch) >= batch_size:
                    _write_batch(batch, writer, fmt, flatten_for_tabular)
                    total_docs += len(batch)
                    batch = []
                    if total_docs % (batch_size * 10) == 0:
                        LOGGER.info(f"Processed {total_docs} documents...")
            if batch:
                _write_batch(batch, writer, fmt, flatten_for_tabular)
                total_docs += len(batch)
    except Exception as exc:
        LOGGER.exception(f"Export failed after {total_docs} docs: {exc}")
        raise
    finally:
        try:
            writer.close()
        except Exception:
            pass

    LOGGER.info(
        f"Export completed. Total documents: {total_docs}. Local file: {local_path}")
    return local_path


def _write_batch(batch: List[Dict[str, Any]], writer: BaseBatchWriter, fmt: str, flatten_for_tabular: bool) -> None:
    sanitized: List[Dict[str, Any]] = [sanitize_document(d) for d in batch]
    if fmt in ("csv", "orc", "avro", "parquet") and flatten_for_tabular:
        sanitized = [flatten_dict(d) for d in sanitized]
    writer.write_batch(sanitized)
