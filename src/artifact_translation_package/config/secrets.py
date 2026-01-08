from databricks.sdk import *
from typing import Optional, Any
import os

# Default scope name - can be overridden via SECRETS_SCOPE env var
DEFAULT_SECRETS_SCOPE = "migration-accelerator"


def get_secret(secret_name):
    """Retrieve secrets from Databricks secret scope"""
    scope = os.getenv("SECRETS_SCOPE", DEFAULT_SECRETS_SCOPE)
    try:
        return dbutils.secrets.get(scope, secret_name)
    except Exception as e:
        # Fallback to environment variables for local development
        return os.getenv(secret_name, "")
