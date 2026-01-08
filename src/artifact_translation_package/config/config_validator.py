"""
Configuration validation utilities.

Provides functions to validate required environment variables and raise
clear error messages when they are missing.
"""

import os
import logging
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

# Required environment variables for different components
REQUIRED_SNOWFLAKE_VARS = [
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

REQUIRED_UC_VARS = [
    "UC_CATALOG",
    "UC_SCHEMA",
]

REQUIRED_SECRETS_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER", 
    "SNOWFLAKE_PASSWORD",
]


class ConfigurationError(Exception):
    """Raised when required configuration is missing."""
    pass


def get_required_env(var_name: str, component: str = "application") -> str:
    """
    Get a required environment variable, raising a clear error if not set.
    
    Args:
        var_name: Name of the environment variable
        component: Component name for error message context
        
    Returns:
        The environment variable value
        
    Raises:
        ConfigurationError: If the variable is not set or empty
    """
    value = os.environ.get(var_name, "").strip()
    if not value:
        error_msg = (
            f"Missing required environment variable: {var_name}\n"
            f"Component: {component}\n"
            f"Please set this variable in your cluster configuration or .env file.\n"
            f"See env.example for reference."
        )
        logger.error(error_msg)
        raise ConfigurationError(error_msg)
    return value


def get_env_with_fallback(var_name: str, fallback: str, component: str = "application") -> str:
    """
    Get environment variable with fallback, logging a warning if using fallback.
    
    Args:
        var_name: Name of the environment variable
        fallback: Fallback value if not set
        component: Component name for logging context
        
    Returns:
        The environment variable value or fallback
    """
    value = os.environ.get(var_name, "").strip()
    if not value:
        if fallback:
            logger.warning(
                f"Environment variable {var_name} not set for {component}, "
                f"using fallback: '{fallback}'"
            )
            return fallback
        else:
            logger.warning(
                f"Environment variable {var_name} not set for {component}, "
                f"no fallback available - this may cause errors"
            )
            return ""
    return value


def validate_snowflake_config() -> Dict[str, str]:
    """
    Validate and return Snowflake configuration.
    
    Returns:
        Dict with validated Snowflake config
        
    Raises:
        ConfigurationError: If required variables are missing
    """
    missing = []
    config = {}
    
    for var in REQUIRED_SNOWFLAKE_VARS:
        value = os.environ.get(var, "").strip()
        if not value:
            missing.append(var)
        else:
            config[var] = value
    
    if missing:
        error_msg = (
            f"Missing required Snowflake configuration:\n"
            f"  - {chr(10).join(missing)}\n\n"
            f"Please set these environment variables in your cluster configuration.\n"
            f"See env.example for reference."
        )
        logger.error(error_msg)
        raise ConfigurationError(error_msg)
    
    logger.info(f"Snowflake config validated: database={config.get('SNOWFLAKE_DATABASE')}, "
                f"schema={config.get('SNOWFLAKE_SCHEMA')}")
    return config


def validate_unity_catalog_config() -> Dict[str, str]:
    """
    Validate and return Unity Catalog configuration.
    
    Returns:
        Dict with validated UC config
        
    Raises:
        ConfigurationError: If required variables are missing
    """
    missing = []
    config = {}
    
    for var in REQUIRED_UC_VARS:
        value = os.environ.get(var, "").strip()
        if not value:
            missing.append(var)
        else:
            config[var] = value
    
    # RAW_VOLUME has a default
    config["UC_RAW_VOLUME"] = os.environ.get("UC_RAW_VOLUME", "snowflake_artifacts_raw")
    
    if missing:
        error_msg = (
            f"Missing required Unity Catalog configuration:\n"
            f"  - {chr(10).join(missing)}\n\n"
            f"Please set these environment variables in your cluster configuration.\n"
            f"See env.example for reference."
        )
        logger.error(error_msg)
        raise ConfigurationError(error_msg)
    
    logger.info(f"Unity Catalog config validated: catalog={config.get('UC_CATALOG')}, "
                f"schema={config.get('UC_SCHEMA')}")
    return config


def get_uc_volume_path_validated() -> str:
    """
    Get the Unity Catalog volume path with validation.
    
    Returns:
        Validated volume path
        
    Raises:
        ConfigurationError: If required UC variables are missing
    """
    config = validate_unity_catalog_config()
    return f"/Volumes/{config['UC_CATALOG']}/{config['UC_SCHEMA']}/{config['UC_RAW_VOLUME']}"


def log_config_summary():
    """Log a summary of current configuration for debugging."""
    env_vars = [
        "UC_CATALOG", "UC_SCHEMA", "UC_RAW_VOLUME",
        "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
        "SECRETS_SCOPE", "DBX_ENDPOINT", "DDL_OUTPUT_DIR"
    ]
    
    logger.info("=" * 60)
    logger.info("Configuration Summary:")
    logger.info("=" * 60)
    
    for var in env_vars:
        value = os.environ.get(var, "")
        if value:
            # Mask sensitive values
            if "SECRET" in var or "PASSWORD" in var:
                value = "***"
            logger.info(f"  {var}: {value}")
        else:
            logger.warning(f"  {var}: NOT SET")
    
    logger.info("=" * 60)
