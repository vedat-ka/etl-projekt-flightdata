import logging
from pathlib import Path


def setup_logging(log_path: Path) -> logging.Logger:
    """Configure console and file logging for the ETL pipeline."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("flight_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger