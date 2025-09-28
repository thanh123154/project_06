import os
import json
import pymongo
from typing import Optional, List, Dict, Any
from datetime import datetime


def get_logger(name: str):
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(name)


def clean_doc(doc: dict) -> dict:
    """Chuẩn hóa document trước khi ghi ra file JSONL"""
    def _fix_value(v):
        if v is None:
            return None
        if isinstance(v, bool):
            # chuyển boolean thành string "true"/"false"
            return str(v).lower()
        if isinstance(v, list):
            return [_fix_value(x) for x in v]
        if isinstance(v, dict):
            return {k: _fix_value(val) for k, val in v.items()}
        return v

    cleaned = {}
    for k, v in doc.items():
        # ép option luôn thành list
        if k == "option" and isinstance(v, dict):
            cleaned[k] = [v]
        else:
            cleaned[k] = _fix_value(v)
    return cleaned


def load_query(query_json: Optional[str], query_file: Optional[str]) -> Dict[str, Any]:
    if query_json:
        return json.loads(query_json)
    if query_file:
        with open(query_file, "r") as f:
            return json.load(f)
    return {}


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
    flatten_for_tabular: bool = False,
) -> str:
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    coll = db[collection_name]

    os.makedirs(local_output_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(
        local_output_dir, f"{file_prefix}_{timestamp}.{fmt}")

    cursor = coll.find(query, projection=fields, batch_size=batch_size)

    with open(output_path, "w", encoding="utf-8") as f:
        for doc in cursor:
            cleaned = clean_doc(doc)
            f.write(json.dumps(cleaned, ensure_ascii=False) + "\n")

    return output_path


def upload_to_gcs(local_path: str, bucket: str, dest_blob: str) -> str:
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket.name}/{dest_blob}"
