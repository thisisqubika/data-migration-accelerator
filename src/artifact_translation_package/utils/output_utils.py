"""Helpers for building timestamped output paths for Databricks and local filesystems."""
import os
from datetime import datetime
from typing import Optional


def utc_timestamp() -> str:
    """Return a UTC timestamp suitable for filesystem names.

    Format: YYYY-MM-DDTHH-MM-SSZ (colons replaced with hyphens to be safe in filenames)
    """
    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def make_timestamped_output_path(output_base: Optional[str], output_format: str) -> Optional[str]:
    """Return a timestamped output path.

    - If `output_base` is falsy, returns None.
    - If `output_base` starts with `dbfs:/` the returned path keeps that prefix.
    - For `json` format, returns a path to a file named `results.json` inside a timestamped folder.
    - For `sql` format, returns a timestamped directory path (ends with `/` for dbfs or normal dir path).
    """
    if not output_base:
        return None

    ts = utc_timestamp()

    # Normalize base (strip trailing slash for consistent joining)
    if output_base.startswith("dbfs:/"):
        base = output_base.rstrip("/")
        if output_format == "json":
            return f"{base}/{ts}/results.json"
        else:
            # leave trailing slash to indicate directory on dbfs
            return f"{base}/{ts}/"
    else:
        base = output_base.rstrip(os.path.sep)
        if output_format == "json":
            return os.path.join(base, ts, "results.json")
        else:
            return os.path.join(base, ts)
