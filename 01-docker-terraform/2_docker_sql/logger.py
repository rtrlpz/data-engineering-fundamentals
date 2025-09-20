# logger.py
import logging
import sys
from logging.handlers import RotatingFileHandler

def get_logger(name: str, log_file: str = "/logs/ingestion.log") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(console)

    # Rotating file handler
    file_handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    return logger
