import time
from typing import Optional

from .logging_utils import get_logger

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover
    storage = None

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
