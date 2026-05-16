# src/common/logging.py
import logging, os, sys

def setup_logger(name: str = "app") -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # tránh add handler nhiều lần

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"))

    logger.addHandler(handler)
    logger.propagate = False
    return logger
