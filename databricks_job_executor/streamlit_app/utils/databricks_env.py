"""
Databricks environment utilities for job executor.
"""
import os
from typing import Dict, Any, Optional

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    dbutils = None


def initialize_databricks_environment() -> Dict[str, Any]:
    """
    Initialize Databricks environment configuration.

    Returns:
        Dict containing environment configuration
    """
    return {
        'host': os.getenv('DATABRICKS_HOST', ''),
        'token': os.getenv('DATABRICKS_TOKEN', ''),
        'is_databricks_runtime': _is_databricks_runtime(),
        'workspace_id': os.getenv('DATABRICKS_WORKSPACE_ID', ''),
        'bundle_environment': os.getenv('DATABRICKS_BUNDLE_ENV', 'dev'),
    }


def _is_databricks_runtime() -> bool:
    """Check if running in Databricks runtime environment."""
    return dbutils is not None and 'DATABRICKS_RUNTIME_VERSION' in os.environ


def get_databricks_client(host: str, token: str):
    """
    Get Databricks client for API calls.

    Args:
        host: Databricks workspace URL
        token: Access token

    Returns:
        Databricks client instance
    """
    try:
        from databricks.sdk import WorkspaceClient
        if _is_databricks_runtime():
            # In Databricks runtime, try to get host and token from widgets or secrets
            host = dbutils.widgets.get("databricks_host") if dbutils.widgets.get("databricks_host") else host
            token = dbutils.widgets.get("databricks_token") if dbutils.widgets.get("databricks_token") else token
            if not token and dbutils.secrets.get("databricks-token-scope", "databricks-token-key"):
                token = dbutils.secrets.get("databricks-token-scope", "databricks-token-key")
        return WorkspaceClient(host=host, token=token)
    except ImportError:
        # Fallback for environments without databricks-sdk
        return None
    except Exception as e:
        print(f"Error getting Databricks client: {e}")
        return None


def validate_connection(host: str, token: str) -> tuple[bool, str]:
    """
    Validate Databricks connection.

    Args:
        host: Databricks workspace URL
        token: Access token

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not host or not token:
        return False, "Host and token are required"

    if not host.startswith('https://'):
        return False, "Host must start with https://"

    try:
        client = get_databricks_client(host, token)
        if client:
            # Test connection by trying to get current user
            client.current_user.me()
            return True, ""
        else:
            return False, "Databricks SDK not available"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"