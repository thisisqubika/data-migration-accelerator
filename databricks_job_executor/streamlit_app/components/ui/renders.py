"""
Rendering functions for the main application UI.
"""
import streamlit as st
from streamlit_app.components.job_interface import JobInterface
from streamlit_app.components.ui.styles import get_header_styles
from streamlit_app.utils.databricks_env import validate_connection


def render_sidebar():
    """Render the sidebar with connection status."""
    with st.sidebar:
        st.markdown("## ⚙️ Configuration")
        
        st.markdown("### Connection Status")
        
        host = st.session_state.get('databricks_host', '')
        client_id = st.session_state.get('databricks_client_id', '')
        client_secret = st.session_state.get('databricks_client_secret', '')
        
        job_id = st.session_state.get('databricks_job_id')
        
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
            st.markdown(f"Please set the following environment variables:\n- " + "\n- ".join(missing))
        
        st.divider()
        
        st.markdown("### ℹ️ About")
        st.markdown("""
        **Data Migration Accelerator**
        
        This tool helps you:
        - Execute the configured migration job
        - Monitor job runs and progress
        - View job logs and diagnostics
        - Cancel running jobs if needed
        
        **Configuration:**
        Set via environment variables:
        - `DATABRICKS_HOST` - Workspace URL
        - `DATABRICKS_CLIENT_ID` - Service principal client ID
        - `DATABRICKS_CLIENT_SECRET` - Service principal client secret
        - `DATABRICKS_JOB_ID` - Job ID to run
        """)


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


def render_main_content():
    """Render the main content area of the application."""
    render_sidebar()
    render_header()
    
    host = st.session_state.get('databricks_host', '')
    client_id = st.session_state.get('databricks_client_id', '')
    client_secret = st.session_state.get('databricks_client_secret', '')
    
    if not host or not client_id or not client_secret:
        st.error("⚠️ **Configuration Required**")
        st.markdown("""
        Please set the following environment variables before running the application:
        
        - `DATABRICKS_HOST` - Your Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
        - `DATABRICKS_CLIENT_ID` - Your service principal client ID
        - `DATABRICKS_CLIENT_SECRET` - Your service principal client secret
        
        You can set these in your environment or in a `.env` file.
        """)
        return
    
    is_valid, error_msg = validate_connection(host, client_id, client_secret)
    if not is_valid:
        st.error(f"❌ **Connection Failed**")
        st.error(f"Unable to connect to Databricks: {error_msg}")
        st.info("Please check your `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` environment variables.")
        return
    
    job_interface = JobInterface()
    job_interface.render()


def render_footer():
    """Render the application footer."""
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 1rem;'>"
        "Data Migration Accelerator | Built with Streamlit"
        "</div>",
        unsafe_allow_html=True
    )

