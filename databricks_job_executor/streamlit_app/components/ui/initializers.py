import os
import streamlit as st
from streamlit_app.utils.databricks_env import initialize_databricks_environment
from streamlit_app.components.ui.styles import get_page_config_styles


def configure_page(bundle_environment: str = 'dev'):
    """Configure Streamlit page settings for migration accelerator interface."""
    page_title = "Data Migration Accelerator"
    if bundle_environment != 'prod':
        page_title += f" ({bundle_environment.capitalize()})"

    st.set_page_config(
        page_title=page_title,
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Apply page configuration styles
    st.markdown(get_page_config_styles(), unsafe_allow_html=True)


def initialize_config_state(db_env: dict):
    """Initialize configuration state from environment variables or Databricks environment."""
    st.session_state.databricks_host = db_env.get('host', '')
    st.session_state.databricks_token = db_env.get('token', '')
    st.session_state.bundle_environment = db_env.get('bundle_environment', 'dev')

    job_id_str = os.getenv('DATABRICKS_JOB_ID') # Still allow .env override
    if not job_id_str and db_env.get('is_databricks_runtime'):
        # In Databricks runtime, try to get job ID from widgets
        from databricks.sdk.runtime import dbutils
        try:
            job_id_str = dbutils.widgets.get("databricks_job_id")
        except Exception:
            pass

    if job_id_str:
        try:
            st.session_state.databricks_job_id = int(job_id_str)
        except ValueError:
            st.session_state.databricks_job_id = None
    else:
        st.session_state.databricks_job_id = None


def initialize_job_state():
    """Initialize job-related state variables."""
    if 'jobs_list' not in st.session_state:
        st.session_state.jobs_list = []

    if 'selected_job' not in st.session_state:
        st.session_state.selected_job = None

    if 'running_jobs' not in st.session_state:
        st.session_state.running_jobs = {}

    if 'job_logs' not in st.session_state:
        st.session_state.job_logs = {}


def initialize_environment_state(db_env: dict):
    """Initialize environment-related state."""
    if 'databricks_env' not in st.session_state:
        st.session_state.databricks_env = db_env


def initialize_session_state(db_env: dict):
    """Initialize Streamlit session state variables."""
    initialize_config_state(db_env)
    initialize_job_state()
    initialize_environment_state(db_env)