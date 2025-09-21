import os
import sys
import logging
from logging.handlers import RotatingFileHandler


def get_logger(name: str = "export_to_gcs") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)

    log_dir = os.path.join(os.getcwd(), "logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(os.path.join(
            log_dir, "export.log"), maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
        logger.addHandler(file_handler)
    except Exception:
        pass

    return logger
