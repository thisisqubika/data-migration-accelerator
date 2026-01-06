"""
Rendering functions for the main application UI.
"""
import streamlit as st
from streamlit_app.components.job_interface import JobInterface
from streamlit_app.components.ui.styles import get_header_styles
from streamlit_app.utils.databricks_env import validate_connection


def _get_session_config():
    """Extract connection configuration from session state."""
    return {
        'host': st.session_state.get('databricks_host', ''),
        'client_id': st.session_state.get('databricks_client_id', ''),
        'client_secret': st.session_state.get('databricks_client_secret', ''),
        'is_runtime': st.session_state.get('databricks_env', {}).get('is_databricks_runtime', False),
        'job_id': st.session_state.get('databricks_job_id'),
    }


def _render_connection_status_runtime(job_id):
    """Render connection status for Databricks runtime environment."""
    is_valid, error_msg = validate_connection()
    if is_valid:
        st.success("✅ Connected to Databricks")
        st.info("**Environment:** Databricks Runtime")
        if job_id:
            st.info(f"**Job ID:**\n`{job_id}`")
        else:
            st.warning("⚠️ No Job ID configured")
    else:
        st.error("❌ Connection Failed")
        st.error(f"**Error:** {error_msg}")


def _render_connection_status_local(host, client_id, client_secret, job_id):
    """Render connection status for local development environment."""
    if host and client_id and client_secret:
        is_valid, error_msg = validate_connection(host, client_id, client_secret)
        if is_valid:
            st.success("✅ Connected to Databricks")
            st.info(f"**Workspace:**\n{host}")
            if job_id:
                st.info(f"**Job ID:**\n`{job_id}`")
            else:
                st.warning("⚠️ No Job ID configured")
        else:
            st.error("❌ Connection Failed")
            st.error(f"**Error:** {error_msg}")
    else:
        st.warning("⚠️ Configuration Missing")
        missing = []
        if not host:
            missing.append("`DATABRICKS_HOST`")
        if not client_id:
            missing.append("`DATABRICKS_CLIENT_ID`")
        if not client_secret:
            missing.append("`DATABRICKS_CLIENT_SECRET`")
        if not job_id:
            missing.append("`DATABRICKS_JOB_ID`")
        st.markdown("Please set the following environment variables:\n- " + "\n- ".join(missing))


def _render_about_section(is_runtime):
    """Render the About section in the sidebar."""
    st.markdown("### ℹ️ About")
    st.markdown("""
    **Data Migration Accelerator**
    
    This tool helps you:
    - Execute the configured migration job
    - Monitor job runs and progress
    - View job logs and diagnostics
    - Cancel running jobs if needed
    """)
    
    if is_runtime:
        st.markdown("""
        **Deployed in Databricks Runtime**
        - Authentication: Automatic
        - Configure `DATABRICKS_JOB_ID` to set default job
        """)
    else:
        st.markdown("""
        **Local Development Configuration:**
        - `DATABRICKS_HOST` - Workspace URL
        - `DATABRICKS_CLIENT_ID` - Service principal client ID
        - `DATABRICKS_CLIENT_SECRET` - Service principal client secret
        - `DATABRICKS_JOB_ID` - Job ID to run
        """)


def render_sidebar():
    """Render the sidebar with connection status."""
    config = _get_session_config()
    
    with st.sidebar:
        st.markdown("## ⚙️ Configuration")
        st.markdown("### Connection Status")
        
        if config['is_runtime']:
            _render_connection_status_runtime(config['job_id'])
        else:
            _render_connection_status_local(
                config['host'], config['client_id'], 
                config['client_secret'], config['job_id']
            )
        
        st.divider()
        _render_about_section(config['is_runtime'])


def render_header():
    """Render the application header."""
    st.markdown(get_header_styles(), unsafe_allow_html=True)
    
    st.markdown("""
    <div class="fancy-header">
        <div class="logo-container">
            <svg width="80" height="80" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M12 2L2 7L12 12L22 7L12 2Z" fill="#FF4B4B"/>
                <path d="M2 17L12 22L22 17V12L12 17L2 12V17Z" fill="#FF4B4B"/>
            </svg>
        </div>
        <div class="title-container">
            <h1 class="main-title">Data Migration Accelerator</h1>
            <p class="subtitle">Execute and Monitor Databricks Migration Jobs</p>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _check_connection_and_render_errors(config) -> bool:
    """Check connection and render appropriate error messages. Returns True if connected."""
    if config['is_runtime']:
        is_valid, error_msg = validate_connection()
        if not is_valid:
            st.error("❌ **Connection Failed**")
            st.error(f"Unable to connect to Databricks: {error_msg}")
            return False
        return True
    
    if not all([config['host'], config['client_id'], config['client_secret']]):
        st.error("⚠️ **Configuration Required**")
        st.markdown("""
        Please set the following environment variables:
        
        - `DATABRICKS_HOST` - Your Databricks workspace URL
        - `DATABRICKS_CLIENT_ID` - Your service principal client ID
        - `DATABRICKS_CLIENT_SECRET` - Your service principal client secret
        
        You can set these in your environment or in a `.env` file.
        """)
        return False
    
    is_valid, error_msg = validate_connection(
        config['host'], config['client_id'], config['client_secret']
    )
    if not is_valid:
        st.error("❌ **Connection Failed**")
        st.error(f"Unable to connect to Databricks: {error_msg}")
        st.info("Please verify your environment variables are correct.")
        return False
    
    return True


def render_main_content():
    """Render the main content area of the application."""
    render_sidebar()
    render_header()
    
    config = _get_session_config()
    
    if not _check_connection_and_render_errors(config):
        return
    
    JobInterface().render()


def render_footer():
    """Render the application footer."""
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 1rem;'>"
        "Data Migration Accelerator | Built with Streamlit"
        "</div>",
        unsafe_allow_html=True
    )
