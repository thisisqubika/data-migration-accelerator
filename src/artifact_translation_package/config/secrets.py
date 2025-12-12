from databricks.sdk import *
from typing import Optional, Any
import os


def get_secret(secret_name):
    """Retrieve secrets from Databricks secret scope"""
    try:
        return dbutils.secrets.get("migration-accelerator", secret_name)
    except Exception as e:
        # Fallback to environment variables for local development
        return os.getenv(secret_name, "")
