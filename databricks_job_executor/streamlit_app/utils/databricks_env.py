"""
Databricks environment utilities for job executor.

Provides authentication and connection management for both:
- Local development (service principal with client_id/client_secret)
- Databricks runtime (automatic built-in authentication)
"""
import os
from typing import Dict, Any, Tuple

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    dbutils = None


def is_databricks_runtime() -> bool:
    """Check if running in Databricks runtime environment."""
    return dbutils is not None and 'DATABRICKS_RUNTIME_VERSION' in os.environ


def initialize_databricks_environment() -> Dict[str, Any]:
    """Initialize and return Databricks environment configuration."""
    return {
        'host': os.getenv('DATABRICKS_HOST', ''),
        'client_id': os.getenv('DATABRICKS_CLIENT_ID', ''),
        'client_secret': os.getenv('DATABRICKS_CLIENT_SECRET', ''),
        'is_databricks_runtime': is_databricks_runtime(),
        'workspace_id': os.getenv('DATABRICKS_WORKSPACE_ID', ''),
        'bundle_environment': os.getenv('DATABRICKS_BUNDLE_ENV', 'dev'),
    }


def _create_runtime_client():
    """Create WorkspaceClient using Databricks runtime's built-in auth."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _create_service_principal_client(host: str, client_id: str, client_secret: str):
    """Create WorkspaceClient using service principal OAuth credentials."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)


def get_databricks_client(host: str = "", client_id: str = "", client_secret: str = ""):
    """
    Get Databricks WorkspaceClient with appropriate authentication.
    
    In Databricks runtime: Uses automatic authentication (no credentials needed).
    Locally: Uses service principal OAuth (requires all three parameters).
    
    Returns:
        WorkspaceClient instance or None if unavailable.
    """
    try:
        if is_databricks_runtime():
            return _create_runtime_client()
        
        if host and client_id and client_secret:
            return _create_service_principal_client(host, client_id, client_secret)
        
        return None
    except ImportError:
        return None
    except Exception as e:
        print(f"Error creating Databricks client: {e}")
        return None


def _test_connection(client) -> Tuple[bool, str]:
    """Test if client can successfully authenticate."""
    try:
        client.current_user.me()
        return True, ""
    except Exception as e:
        return False, f"Connection failed: {str(e)}"


def validate_connection(host: str = "", client_id: str = "", client_secret: str = "") -> Tuple[bool, str]:
    """
    Validate Databricks connection.
    
    Returns:
        Tuple of (is_valid, error_message).
    """
    if is_databricks_runtime():
        client = get_databricks_client()
        if not client:
            return False, "Databricks SDK not available"
        return _test_connection(client)
    
    if not all([host, client_id, client_secret]):
        return False, "Host, client_id, and client_secret are required"

    if not host.startswith('https://'):
        return False, "Host must start with https://"

    client = get_databricks_client(host, client_id, client_secret)
    if not client:
        return False, "Failed to create Databricks client"
    
    return _test_connection(client)


# Keep backward compatibility with internal function name
_is_databricks_runtime = is_databricks_runtime