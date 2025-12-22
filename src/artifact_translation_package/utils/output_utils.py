"""Helpers for building timestamped output paths for Databricks and local filesystems.

This module also contains a small runtime detection helper so callers may
keep Databricks-style paths (dbfs:/...) as the canonical configuration while
mapping those paths to local directories when executed outside Databricks.
"""
import os
from datetime import datetime
from typing import Optional


def utc_timestamp() -> str:
    """Return a UTC timestamp suitable for filesystem names.

    Format: YYYY-MM-DDTHH-MM-SSZ (colons replaced with hyphens to be safe in filenames)
    """
    return datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")


def is_databricks_env() -> bool:
    """Return True when running inside a Databricks runtime.

    Detection strategy:
    - If the environment variable `DATABRICKS_RUNTIME_VERSION` exists, assume Databricks.
    - Otherwise try importing `dbutils` which is available in Databricks notebooks.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    try:
        import dbutils  # type: ignore

        return True
    except Exception:
        return False


def make_timestamped_output_path(output_base: Optional[str], output_format: str) -> Optional[str]:
    """Return a timestamped output path.

    Behavior:
    - If `output_base` is falsy, returns None.
    - If `output_base` starts with `/Workspace/` raises ValueError (not writable).
    - If `output_base` starts with `dbfs:/` and we're running on Databricks, return a
      dbfs-style path with the timestamp preserved (same as before).
    - If `output_base` starts with `dbfs:/` and we're NOT on Databricks, map the base
      to a local directory defined by `LOCAL_DBFS_MOUNT` env var (defaults to `./ddl_output`).
    - For `json` format, returns a path to a file named `results.json` inside a timestamped folder.
    - For `sql` format, returns a timestamped directory path.
    """
    if not output_base:
        return None

    # Disallow Workspace paths which are not writable as normal filesystem locations.
    if output_base.startswith("/Workspace/"):
        raise ValueError(
            "Invalid DDL_OUTPUT_PATH: '/Workspace/...' is not a writable filesystem path. "
            "Use a DBFS path (dbfs:/...) or a mounted Volume (/Volumes/...) instead."
        )

    ts = utc_timestamp()

    # If it's a DBFS path but we are running locally, map it to a local mount/output dir.
    if output_base.startswith("dbfs:/") and not is_databricks_env():
        local_base = os.environ.get("LOCAL_DBFS_MOUNT", "./ddl_output")
        # normalize local_base
        local_base = local_base.rstrip(os.path.sep)
        if output_format == "json":
            return os.path.join(local_base, ts, "results.json")
        else:
            return os.path.join(local_base, ts)

    # Running in Databricks or not a dbfs path: preserve original semantics
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


