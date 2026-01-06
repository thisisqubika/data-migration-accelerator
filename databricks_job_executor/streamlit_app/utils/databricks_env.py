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
        'client_id': os.getenv('DATABRICKS_CLIENT_ID', ''),
        'client_secret': os.getenv('DATABRICKS_CLIENT_SECRET', ''),
        'is_databricks_runtime': _is_databricks_runtime(),
        'workspace_id': os.getenv('DATABRICKS_WORKSPACE_ID', ''),
        'bundle_environment': os.getenv('DATABRICKS_BUNDLE_ENV', 'dev'),
    }


def _is_databricks_runtime() -> bool:
    """Check if running in Databricks runtime environment."""
    return dbutils is not None and 'DATABRICKS_RUNTIME_VERSION' in os.environ


def get_databricks_client(host: str, client_id: str, client_secret: str):
    """
    Get Databricks client for API calls using service principal authentication.

    Args:
        host: Databricks workspace URL
        client_id: Service principal client ID
        client_secret: Service principal client secret

    Returns:
        Databricks client instance
    """
    try:
        from databricks.sdk import WorkspaceClient
        if _is_databricks_runtime():
            # In Databricks runtime, try to get credentials from widgets or secrets
            host = dbutils.widgets.get("databricks_host") if dbutils.widgets.get("databricks_host") else host
            client_id = dbutils.widgets.get("databricks_client_id") if dbutils.widgets.get("databricks_client_id") else client_id
            client_secret = dbutils.widgets.get("databricks_client_secret") if dbutils.widgets.get("databricks_client_secret") else client_secret
            if not client_secret and dbutils.secrets.get("databricks-sp-scope", "databricks-client-secret"):
                client_secret = dbutils.secrets.get("databricks-sp-scope", "databricks-client-secret")
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    except ImportError:
        # Fallback for environments without databricks-sdk
        return None
    except Exception as e:
        print(f"Error getting Databricks client: {e}")
        return None


def validate_connection(host: str, client_id: str, client_secret: str) -> tuple[bool, str]:
    """
    Validate Databricks connection using service principal authentication.

    Args:
        host: Databricks workspace URL
        client_id: Service principal client ID
        client_secret: Service principal client secret

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not host or not client_id or not client_secret:
        return False, "Host, client_id, and client_secret are required"

    if not host.startswith('https://'):
        return False, "Host must start with https://"

    try:
        client = get_databricks_client(host, client_id, client_secret)
        if client:
            # Test connection by trying to get current user
            client.current_user.me()
            return True, ""
        else:
            return False, "Databricks SDK not available"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"