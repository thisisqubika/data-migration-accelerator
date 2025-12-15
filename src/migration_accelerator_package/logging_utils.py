"""
Lightweight application logging helpers for Databricks jobs.

We use Python's standard logging but enforce a consistent, searchable format
so logs are easy to find in Databricks driver / cluster stdout.
"""

from __future__ import annotations

import logging
import os
from typing import Optional


APP_TAG = "MIGRATION_ACCELERATOR"


def _ensure_basic_config() -> None:
    """
    Ensure the root logger has at least a basic configuration.

    Databricks often configures logging, but in case it's not configured for
    the Python process we set a simple StreamHandler to stdout.
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
        )


def get_app_logger(component: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a logger configured with a consistent, searchable format.

    Log format example:
        [MIGRATION_ACCELERATOR][snowpark-reader][INFO] Some message

    Args:
        component: Short name of the component / entrypoint (e.g. 'snowpark-reader')
        level: Optional log level name (e.g. 'DEBUG', 'INFO'). If not provided,
               uses LOG_LEVEL env var or defaults to INFO.
    """
    _ensure_basic_config()

    logger_name = f"{APP_TAG}.{component}"
    logger = logging.getLogger(logger_name)

    # Configure level
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    try:
        logger.setLevel(getattr(logging, level.upper()))
    except AttributeError:
        logger.setLevel(logging.INFO)

    # Install a formatter with our tag if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt=f"[{APP_TAG}][{component}][%(levelname)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Avoid double logging via ancestor handlers
    logger.propagate = False

    return logger


