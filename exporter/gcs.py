import json
import os
import time
from typing import Optional

from .logging_utils import get_logger

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover
    storage = None

try:
    from bson import decode as bson_decode  # type: ignore
except ImportError:
    try:
        from pymongo.bson import decode as bson_decode  # type: ignore
    except ImportError:
        bson_decode = None

LOGGER = get_logger("gcs")


def upload_to_gcs(local_path: str, bucket_name: str, destination_path: str, max_retries: int = 5) -> str:
    if storage is None:
        raise RuntimeError(
            "google-cloud-storage is not installed. Install and set credentials to upload.")

    client = storage.Client()  # relies on GOOGLE_APPLICATION_CREDENTIALS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)

    attempt = 0
    while True:
        try:
            blob.upload_from_filename(local_path)
            LOGGER.info(f"Uploaded to gs://{bucket_name}/{destination_path}")
            return f"gs://{bucket_name}/{destination_path}"
        except Exception as exc:
            attempt += 1
            if attempt >= max_retries:
                LOGGER.error(
                    f"GCS upload failed after {attempt} attempts: {exc}")
                raise
            sleep_s = min(60, 2 ** attempt)
            LOGGER.warning(
                f"GCS upload failed (attempt {attempt}). Retrying in {sleep_s}s: {exc}")
            time.sleep(sleep_s)


def download_bson_from_gcs(bucket_name: str, source_path: str, local_output_path: Optional[str] = None) -> str:
    """
    Download a BSON file from GCS, convert it to JSONL format, and save locally.
    
    Args:
        bucket_name: GCS bucket name
        source_path: Path to BSON file in GCS
        local_output_path: Optional path for output JSONL file. If None, uses same name with .jsonl extension
    
    Returns:
        Path to the local JSONL file
    """
    if storage is None:
        raise RuntimeError(
            "google-cloud-storage is not installed. Install and set credentials to download.")
    
    if bson_decode is None:
        raise RuntimeError(
            "bson is not available. Make sure pymongo is installed")
    
    # Setup output path
    if local_output_path is None:
        base_name = os.path.splitext(os.path.basename(source_path))[0]
        local_output_path = f"{base_name}.jsonl"
    
    LOGGER.info(f"Downloading BSON from gs://{bucket_name}/{source_path}")
    
    client = storage.Client()  # relies on GOOGLE_APPLICATION_CREDENTIALS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_path)
    
    # Download BSON file to memory
    bson_data = blob.download_as_bytes()
    LOGGER.info(f"Downloaded {len(bson_data)} bytes of BSON data")
    
    # Convert BSON to JSONL
    LOGGER.info("Converting BSON to JSONL...")
    output_dir = os.path.dirname(local_output_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    
    with open(local_output_path, 'w', encoding='utf-8') as outfile:
        offset = 0
        doc_count = 0
        
        while offset < len(bson_data):
            try:
                # Read document length (BSON stores length as 32-bit integer)
                if offset + 4 > len(bson_data):
                    break
                
                # BSON length is little-endian 32-bit int
                doc_len = int.from_bytes(bson_data[offset:offset+4], byteorder='little')
                
                if doc_len <= 0 or offset + doc_len > len(bson_data):
                    break
                
                # Extract document
                doc_bytes = bson_data[offset:offset + doc_len]
                doc = bson_decode(doc_bytes)
                
                # Write as JSON line
                json.dump(doc, outfile, ensure_ascii=False, default=str)
                outfile.write('\n')
                
                offset += doc_len
                doc_count += 1
                
                if doc_count % 10000 == 0:
                    LOGGER.info(f"Converted {doc_count} documents...")
                    
            except Exception as exc:
                LOGGER.warning(f"Error decoding document at offset {offset}: {exc}")
                break
    
    LOGGER.info(f"Converted {doc_count} documents. Output: {local_output_path}")
    return local_output_path
