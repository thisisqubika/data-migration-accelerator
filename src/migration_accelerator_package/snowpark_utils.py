"""
Utility functions shared across ingestion + validation entrypoints.
"""

import os
from databricks.sdk.runtime import dbutils
from migration_accelerator_package.constants import SnowflakeConfig, UnityCatalogConfig


def get_secret(secret_name: str):
    """Retrieve secrets from Databricks secret scope or fallback to env variables."""
    try:
        return dbutils.secrets.get("migration-accelerator", secret_name)
    except Exception:
        return os.getenv(secret_name, "")


def build_snowflake_connection_params():
    """Return Snowflake connection parameters used by all wheel entrypoints."""
    return {
        "account": get_secret("SNOWFLAKE_ACCOUNT"),
        "user": get_secret("SNOWFLAKE_USER"),
        "password": get_secret("SNOWFLAKE_PASSWORD"),
        "role": SnowflakeConfig.SNOWFLAKE_ROLE.value,
        "warehouse": SnowflakeConfig.SNOWFLAKE_WAREHOUSE.value,
        "database": SnowflakeConfig.SNOWFLAKE_DATABASE.value,
        "schema": SnowflakeConfig.SNOWFLAKE_SCHEMA.value,
    }


def get_uc_volume_path() -> str:
    """Return the base UC volume path where JSON artifacts live."""
    return (
        f"/Volumes/"
        f"{UnityCatalogConfig.CATALOG.value}/"
        f"{UnityCatalogConfig.SCHEMA.value}/"
        f"{UnityCatalogConfig.RAW_VOLUME.value}"
    )

def load_json_from_volume(volume_path: str, filename: str) -> dict:
    """
    Load a JSON artifact file from a Unity Catalog volume using dbutils.fs.head(),
    which is compatible with all cluster types including serverless compute.
    Args:
        volume_path: Base UC volume path
        filename: JSON file name (e.g., 'roles.json')

    Returns:
        Parsed JSON dict. Returns {} if file missing or invalid.
    """
    path = f"{volume_path}/{filename}"

    try:
        raw = dbutils.fs.head(path, 50_000_000)  # 50 MB max
        return json.loads(raw)
    except Exception as e:
        print(f"  ⚠ Warning: Could not load {filename}: {e}")
        return {}
