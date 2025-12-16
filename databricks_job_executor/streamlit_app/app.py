"""
Main Streamlit Application for Data Migration Accelerator

Data Migration Accelerator - Execute and Monitor Databricks Jobs
"""

import atexit
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import streamlit as st
from dotenv import load_dotenv

# Add parent directory to Python path to allow absolute imports
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Load environment variables from .env file
env_path = parent_dir / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

from streamlit_app.components.job_interface import JobInterface
from streamlit_app.components.ui.initializers import configure_page, initialize_session_state
from streamlit_app.components.ui.renders import render_main_content, render_footer
from streamlit_app.utils.databricks_env import validate_connection, initialize_databricks_environment


class JobExecutorApp:
    """Main application class for Data Migration Accelerator."""

    def __init__(self):
        """Initialize the application."""
        self._initialize_app()

    def _initialize_app(self):
        """Initialize application components."""
        db_env = initialize_databricks_environment()
        configure_page(db_env.get('bundle_environment', 'dev'))
        initialize_session_state(db_env)

    def _setup_cleanup(self):
        """Setup cleanup handlers."""
        atexit.register(cleanup_temp_data)

    def render(self):
        """Render the complete application."""
        # Main content area
        render_main_content()

        # Footer
        render_footer()

        # Setup cleanup
        self._setup_cleanup()


def cleanup_temp_data():
    """Cleanup temporary data."""
    # Clear any temporary session data if needed
    pass


def _get_job_id() -> Optional[str]:
    """
    Get the Databricks Job ID from environment variables or Databricks widgets.
    """
    job_id = os.getenv('DATABRICKS_JOB_ID')
    if not job_id and dbutils:
        try:
            job_id = dbutils.widgets.get("databricks_job_id")
        except Exception:
            pass
    return job_id


def main():
    """Main application entry point."""
    app = JobExecutorApp()
    app.render()


if __name__ == "__main__":
    main()