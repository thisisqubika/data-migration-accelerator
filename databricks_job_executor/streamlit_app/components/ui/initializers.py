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
    st.session_state.databricks_client_id = db_env.get('client_id', '')
    st.session_state.databricks_client_secret = db_env.get('client_secret', '')
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
        # Restore active runs from Databricks on first load
        _restore_active_runs()

    if 'job_logs' not in st.session_state:
        st.session_state.job_logs = {}


def _restore_active_runs():
    """Query Databricks for active runs of the configured job and restore them."""
    from streamlit_app.utils.job_manager import get_job_manager
    from streamlit_app.utils.formatters import format_timestamp
    
    job_id = st.session_state.get('databricks_job_id')
    if not job_id:
        return
    
    try:
        job_manager = get_job_manager()
        if not job_manager:
            return
        
        active_runs = job_manager.get_active_runs(job_id)
        
        for run_info in active_runs:
            run_id = run_info['run_id']
            start_time_ms = run_info.get('start_time_ms')
            
            st.session_state.running_jobs[run_id] = {
                'job_id': run_info['job_id'],
                'job_name': run_info['job_name'],
                'start_time': format_timestamp(start_time_ms),
                'start_time_ms': start_time_ms,
                'status': run_info['status'],
                'run_id': run_id
            }
    except Exception:
        # Silently fail - don't block initialization
        pass


def initialize_environment_state(db_env: dict):
    """Initialize environment-related state."""
    st.session_state.databricks_env = db_env


def initialize_session_state(db_env: dict):
    """Initialize Streamlit session state variables."""
    initialize_config_state(db_env)
    initialize_job_state()
    initialize_environment_state(db_env)