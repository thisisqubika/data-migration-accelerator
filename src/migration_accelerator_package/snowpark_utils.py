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