"""
Utility functions shared across ingestion + validation entrypoints.
"""

import json
import os
import logging
from databricks.sdk.runtime import dbutils
from migration_accelerator_package.constants import SnowflakeConfig, UnityCatalogConfig
from migration_accelerator_package.logging_utils import get_app_logger

# Module-level logger for utility functions (uses app logger format)
_utils_logger = get_app_logger("utils")

# Default scope name - can be overridden via SECRETS_SCOPE env var
DEFAULT_SECRETS_SCOPE = "migration-accelerator"


class ConfigurationError(Exception):
    """Raised when required configuration is missing."""
    pass


def _get_required_env(var_name: str, fallback: str = "") -> str:
    """
    Get environment variable with validation and logging.
    
    Args:
        var_name: Name of the environment variable
        fallback: Fallback value from constants (may be empty)
        
    Returns:
        The environment variable value
        
    Raises:
        ConfigurationError: If the variable is not set and no fallback
    """
    value = os.environ.get(var_name, "").strip()
    if value:
        return value
    
    # Try fallback from constants
    if fallback:
        _utils_logger.warning(
            f"Environment variable {var_name} not set, using fallback: '{fallback}'"
        )
        return fallback
    
    # No value and no fallback - this is required
    error_msg = (
        f"Missing required environment variable: {var_name}\n"
        f"Please set this variable in your cluster configuration or .env file.\n"
        f"See env.example for reference."
    )
    _utils_logger.error(error_msg)
    raise ConfigurationError(error_msg)


def get_secret(secret_name: str):
    """Retrieve secrets from Databricks secret scope or fallback to env variables."""
    scope = os.getenv("SECRETS_SCOPE", DEFAULT_SECRETS_SCOPE)
    try:
        value = dbutils.secrets.get(scope, secret_name)
        if value:
            return value
    except Exception as e:
        _utils_logger.debug(f"Could not get secret {secret_name} from scope {scope}: {e}")
    
    # Fallback to environment variables
    value = os.getenv(secret_name, "")
    if not value:
        _utils_logger.warning(
            f"Secret {secret_name} not found in scope '{scope}' or environment. "
            f"This may cause authentication failures."
        )
    return value


def build_snowflake_connection_params():
    """
    Return Snowflake connection parameters used by all wheel entrypoints.
    
    Raises:
        ConfigurationError: If required Snowflake config is missing
    """
    # Get credentials from secrets
    account = get_secret("SNOWFLAKE_ACCOUNT")
    user = get_secret("SNOWFLAKE_USER")
    password = get_secret("SNOWFLAKE_PASSWORD")
    
    # Validate credentials
    if not account or not user or not password:
        missing = []
        if not account: missing.append("SNOWFLAKE_ACCOUNT")
        if not user: missing.append("SNOWFLAKE_USER")
        if not password: missing.append("SNOWFLAKE_PASSWORD")
        
        error_msg = (
            f"Missing Snowflake credentials: {', '.join(missing)}\n"
            f"Please configure these in Databricks secrets scope or environment variables."
        )
        _utils_logger.error(error_msg)
        raise ConfigurationError(error_msg)
    
    # Get database/schema with validation
    database = _get_required_env("SNOWFLAKE_DATABASE", SnowflakeConfig.SNOWFLAKE_DATABASE.value)
    schema = _get_required_env("SNOWFLAKE_SCHEMA", SnowflakeConfig.SNOWFLAKE_SCHEMA.value)
    
    _utils_logger.info(f"Snowflake connection: database={database}, schema={schema}")
    
    return {
        "account": account,
        "user": user,
        "password": password,
        "role": os.getenv("SNOWFLAKE_ROLE", SnowflakeConfig.SNOWFLAKE_ROLE.value),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", SnowflakeConfig.SNOWFLAKE_WAREHOUSE.value),
        "database": database,
        "schema": schema,
    }


def get_uc_volume_path() -> str:
    """
    Return the base UC volume path where JSON artifacts live.
    
    Raises:
        ConfigurationError: If required UC config is missing
    """
    catalog = _get_required_env("UC_CATALOG", UnityCatalogConfig.CATALOG.value)
    schema = _get_required_env("UC_SCHEMA", UnityCatalogConfig.SCHEMA.value)
    raw_volume = os.environ.get("UC_RAW_VOLUME", UnityCatalogConfig.RAW_VOLUME.value) or "snowflake_artifacts_raw"
    
    volume_path = f"/Volumes/{catalog}/{schema}/{raw_volume}"
    _utils_logger.info(f"Unity Catalog volume path: {volume_path}")
    
    return volume_path


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
        _utils_logger.warning(f"Could not load {filename}: {e}")
        return {}


def log_config_summary():
    """Log a summary of current configuration for debugging."""
    env_vars = [
        "UC_CATALOG", "UC_SCHEMA", "UC_RAW_VOLUME",
        "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
        "SECRETS_SCOPE", "DBX_ENDPOINT", "DDL_OUTPUT_DIR"
    ]
    
    _utils_logger.info("=" * 60)
    _utils_logger.info("Configuration Summary:")
    _utils_logger.info("=" * 60)
    
    for var in env_vars:
        value = os.environ.get(var, "")
        if value:
            _utils_logger.info(f"  {var}: {value}")
        else:
            _utils_logger.warning(f"  {var}: NOT SET")
    
    _utils_logger.info("=" * 60)
