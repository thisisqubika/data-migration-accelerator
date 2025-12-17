"""
Job execution interface component.
"""
import time
import streamlit as st
from typing import Dict, Any
from streamlit_app.utils.job_manager import get_job_manager


class JobInterface:
    """Interface for job execution and monitoring."""

    def __init__(self):
        self.job_manager = None

    def update_connection(self):
        """Update job manager when connection changes."""
        self.job_manager = get_job_manager()

    def render_job_list(self):
        """Render the configured job section."""
        if not self.job_manager:
            st.warning("Please configure Databricks connection first.")
            return

        configured_job_id = st.session_state.get('databricks_job_id')
        
        if not configured_job_id:
            st.error("❌ **No Job Configured**")
            st.markdown("""
            Please set the `DATABRICKS_JOB_ID` environment variable to specify which job to run.
            
            Example:
            ```bash
            export DATABRICKS_JOB_ID=123456
            ```
            """)
            return
        
        st.markdown("📋 **Configured Job**")
        
        with st.spinner("Loading job details..."):
            job_details = self.job_manager.get_job_details(configured_job_id)
        
        if not job_details:
            st.error(f"❌ Job ID `{configured_job_id}` not found in the workspace.")
            st.warning("Please check your `DATABRICKS_JOB_ID` environment variable.")
            return
        
        st.success(f"✅ Job loaded: **{job_details['settings']['name']}** (ID: {configured_job_id})")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**Job Name:** {job_details['settings']['name']}")
            st.markdown(f"**Job ID:** `{configured_job_id}`")
        with col2:
            st.metric("Timeout", f"{job_details['settings'].get('timeout_seconds', 'N/A')}s")
        
        st.session_state.selected_job = job_details

    def render_job_execution(self):
        """Render job execution controls."""
        if 'selected_job' not in st.session_state or not st.session_state.selected_job:
            return

        job = st.session_state.selected_job
        job_id = job['job_id']
        job_name = job['settings']['name']

        st.markdown("▶️ **Execute Job**")

        # Job details
        with st.expander("Job Details", expanded=False):
            st.json(job)

        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            st.markdown(f"**Selected:** {job_name} (ID: {job_id})")

        with col2:
            if st.button("🚀 Run Job", type="primary", key=f"run_job_{job_id}"):
                self._execute_job(job_id, job_name)

        with col3:
            if st.button("🔄 Refresh Status", key=f"refresh_status_{job_id}"):
                self._refresh_all_runs()

    def _execute_job(self, job_id: int, job_name: str):
        """Execute a job."""
        if not self.job_manager:
            st.error("Job manager not available")
            return

        with st.spinner(f"Starting job '{job_name}'..."):
            run_id = self.job_manager.run_job(job_id)

        if run_id:
            # Store running job info
            st.session_state.running_jobs[run_id] = {
                'job_id': job_id,
                'job_name': job_name,
                'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'PENDING',
                'run_id': run_id
            }

            # Store logs placeholder
            st.session_state.job_logs[run_id] = ""

            st.success(f"Job '{job_name}' started successfully! Run ID: {run_id}")
            time.sleep(1)  # Brief pause before refresh
            st.rerun()
        else:
            st.error(f"Failed to start job '{job_name}'")

    def render_running_jobs(self):
        """Render status of running jobs."""
        if not st.session_state.running_jobs:
            return

        st.markdown("⏳ **Running Jobs**")

        # Update status for all running jobs
        self._update_running_jobs_status()

        for run_id, job_info in list(st.session_state.running_jobs.items()):
            status = job_info.get('status', 'UNKNOWN')
            is_expanded = status in ['PENDING', 'RUNNING']

            with st.expander(f"Run {run_id}: {job_info['job_name']} - {status}", expanded=is_expanded):
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Status", status)

                with col2:
                    st.metric("Start Time", job_info.get('start_time', 'N/A'))

                with col3:
                    duration = job_info.get('execution_duration', 'N/A')
                    if duration and duration != 'N/A':
                        duration = f"{duration // 1000}s"  # Convert ms to seconds
                    st.metric("Duration", duration)

                with col4:
                    if status in ['PENDING', 'RUNNING']:
                        if st.button("❌ Cancel", key=f"cancel_{run_id}"):
                            self._cancel_job(run_id)
                    elif status in ['SUCCESS', 'FAILED', 'CANCELLED']:
                        if st.button("🗑️ Remove", key=f"remove_{run_id}"):
                            del st.session_state.running_jobs[run_id]
                            if run_id in st.session_state.job_logs:
                                del st.session_state.job_logs[run_id]
                            st.rerun()

                # Logs section
                if st.button("📋 View Logs", key=f"logs_{run_id}"):
                    self._show_job_logs(run_id)

    def _update_running_jobs_status(self):
        """Update status for all running jobs."""
        if not self.job_manager:
            return

        for run_id, job_info in list(st.session_state.running_jobs.items()):
            status_info = self.job_manager.get_run_status(run_id)
            if status_info:
                lifecycle_state = status_info['state'].get('life_cycle_state', 'UNKNOWN')
                result_state = status_info['state'].get('result_state')

                # Determine display status
                if lifecycle_state == 'TERMINATED':
                    if result_state == 'SUCCESS':
                        status = 'SUCCESS'
                    elif result_state == 'FAILED':
                        status = 'FAILED'
                    elif result_state == 'CANCELLED':
                        status = 'CANCELLED'
                    else:
                        status = 'TERMINATED'
                elif lifecycle_state == 'RUNNING':
                    status = 'RUNNING'
                elif lifecycle_state == 'PENDING':
                    status = 'PENDING'
                else:
                    status = lifecycle_state or 'UNKNOWN'

                # Update job info
                st.session_state.running_jobs[run_id]['status'] = status
                if status_info.get('execution_duration'):
                    st.session_state.running_jobs[run_id]['execution_duration'] = status_info['execution_duration']

                # Remove completed jobs after some time
                if status in ['SUCCESS', 'FAILED', 'CANCELLED']:
                    # Could add logic to auto-remove after timeout
                    pass

    def _refresh_all_runs(self):
        """Refresh status of all runs."""
        self._update_running_jobs_status()
        st.success("Status refreshed!")

    def _cancel_job(self, run_id: int):
        """Cancel a running job."""
        if not self.job_manager:
            st.error("Job manager not available")
            return

        if self.job_manager.cancel_run(run_id):
            st.session_state.running_jobs[run_id]['status'] = 'CANCELLED'
            st.success(f"Job run {run_id} cancelled successfully!")
        else:
            st.error(f"Failed to cancel job run {run_id}")

    def _show_job_logs(self, run_id: int):
        """Show logs for a job run."""
        if not self.job_manager:
            st.error("Job manager not available")
            return

        with st.spinner("Loading logs..."):
            logs = self.job_manager.get_run_logs(run_id)

        if logs:
            st.session_state.job_logs[run_id] = logs

        # Display logs
        if run_id in st.session_state.job_logs:
            st.code(st.session_state.job_logs[run_id], language='text')
        else:
            st.info("No logs available yet.")

    def render(self):
        """Render the complete job interface."""
        self.update_connection()

        # Job listing
        self.render_job_list()

        # Job execution
        if st.session_state.get('selected_job'):
            st.divider()
            self.render_job_execution()

        # Running jobs status
        if st.session_state.running_jobs:
            st.divider()
            self.render_running_jobs()