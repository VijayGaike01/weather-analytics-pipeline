import logging
from pathlib import Path

Path("logs").mkdir(exist_ok=True)

logger = logging.getLogger("validation_tests")

if not logger.handlers:

    handler = logging.FileHandler(
        "logs/test_validation.log",
        mode="a",
        encoding="utf-8"
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    logger.setLevel(logging.INFO)