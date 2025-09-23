import logging
import os
from logging.handlers import RotatingFileHandler

# Ensure log directory exists
LOG_DIR = "/opt/airflow/custom_persistent_shared/logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "consumer.log")

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Change to INFO or WARNING in production

    # Formatter for logs
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Rotating file handler (max 10MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Prevent duplicate handlers if logger already exists
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
