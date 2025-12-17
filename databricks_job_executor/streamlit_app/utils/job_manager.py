"""
Job management utilities for Databricks Job Executor.
"""
import os
import streamlit as st
from typing import List, Dict, Any, Optional, Tuple
from streamlit_app.utils.databricks_env import get_databricks_client, validate_connection
from streamlit_app.components.ui.handlers import (
    handle_api_error, handle_job_execution_error, handle_job_cancellation_error,
    show_logs_load_error
)


class JobManager:
    """Manager for Databricks job operations."""

    def __init__(self):
        self.client = None
        self._update_client()

    def _update_client(self):
        """Update the Databricks client with current configuration."""
        host = st.session_state.get('databricks_host', '')
        token = st.session_state.get('databricks_token', '')

        if host and token:
            self.client = get_databricks_client(host, token)
        else:
            self.client = None

    def _ensure_client(self) -> bool:
        """Ensure client is available and valid."""
        if not self.client:
            self._update_client()

        if not self.client:
            return False

        # Test connection
        try:
            self.client.current_user.me()
            return True
        except Exception:
            return False

    def get_job_details(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific job.

        Args:
            job_id: The job ID to retrieve

        Returns:
            Job details dictionary or None if failed
        """
        if not self._ensure_client():
            handle_api_error("job details", "No valid Databricks connection")
            return None

        try:
            job = self.client.jobs.get(job_id=job_id)
            
            return {
                'job_id': job.job_id,
                'settings': {
                    'name': job.settings.name if hasattr(job.settings, 'name') else f"Job {job.job_id}",
                    'timeout_seconds': job.settings.timeout_seconds if hasattr(job.settings, 'timeout_seconds') else None,
                    'max_concurrent_runs': job.settings.max_concurrent_runs if hasattr(job.settings, 'max_concurrent_runs') else None,
                },
                'created_time': job.created_time if hasattr(job, 'created_time') else None,
                'creator_user_name': job.creator_user_name if hasattr(job, 'creator_user_name') else None,
            }

        except Exception as e:
            handle_api_error("job details", str(e))
            return None

    def list_jobs(self) -> List[Dict[str, Any]]:
        """
        List all jobs in the Databricks workspace.

        Returns:
            List of job dictionaries
        """
        if not self._ensure_client():
            handle_api_error("job listing", "No valid Databricks connection")
            return []

        try:
            jobs_list = list(self.client.jobs.list())

            formatted_jobs = []
            for job in jobs_list:
                formatted_job = {
                    'job_id': job.job_id,
                    'settings': {
                        'name': job.settings.name if hasattr(job.settings, 'name') else f"Job {job.job_id}",
                        'timeout_seconds': job.settings.timeout_seconds if hasattr(job.settings, 'timeout_seconds') else None,
                        'max_concurrent_runs': job.settings.max_concurrent_runs if hasattr(job.settings, 'max_concurrent_runs') else None,
                    },
                    'created_time': job.created_time if hasattr(job, 'created_time') else None,
                    'creator_user_name': job.creator_user_name if hasattr(job, 'creator_user_name') else None,
                }
                formatted_jobs.append(formatted_job)

            return formatted_jobs

        except Exception as e:
            handle_api_error("job listing", str(e))
            return []

    def run_job(self, job_id: int) -> Optional[int]:
        """
        Run a job by ID.

        Args:
            job_id: The job ID to run

        Returns:
            Run ID if successful, None otherwise
        """
        if not self._ensure_client():
            handle_api_error("job execution", "No valid Databricks connection")
            return None

        try:
            run_response = self.client.jobs.run_now(job_id=job_id)
            run_id = run_response.run_id if hasattr(run_response, 'run_id') else None

            return run_id

        except Exception as e:
            handle_api_error("job execution", str(e))
            return None

    def get_run_status(self, run_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the status of a job run.

        Args:
            run_id: The run ID to check

        Returns:
            Run status information or None if failed
        """
        if not self._ensure_client():
            return None

        try:
            run_info = self.client.jobs.get_run(run_id=run_id)
            
            life_cycle_state = None
            result_state = None
            
            if hasattr(run_info, 'state') and run_info.state:
                if hasattr(run_info.state, 'life_cycle_state') and run_info.state.life_cycle_state:
                    life_cycle_state = str(run_info.state.life_cycle_state.value) if hasattr(run_info.state.life_cycle_state, 'value') else str(run_info.state.life_cycle_state)
                if hasattr(run_info.state, 'result_state') and run_info.state.result_state:
                    result_state = str(run_info.state.result_state.value) if hasattr(run_info.state.result_state, 'value') else str(run_info.state.result_state)
            
            return {
                'state': {
                    'life_cycle_state': life_cycle_state,
                    'result_state': result_state,
                },
                'execution_duration': run_info.execution_duration if hasattr(run_info, 'execution_duration') else None,
                'tasks': run_info.tasks if hasattr(run_info, 'tasks') else [],
            }

        except Exception as e:
            return None

    def cancel_run(self, run_id: int) -> bool:
        """
        Cancel a job run.

        Args:
            run_id: The run ID to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        if not self._ensure_client():
            handle_api_error("job cancellation", "No valid Databricks connection")
            return False

        try:
            self.client.jobs.cancel_run(run_id=run_id)
            return True

        except Exception as e:
            handle_api_error("job cancellation", str(e))
            return False

    def get_run_logs(self, run_id: int) -> Optional[str]:
        """
        Get comprehensive logs for a job run using multiple strategies.

        Args:
            run_id: The run ID to get logs for

        Returns:
            Log content as string or None if failed
        """
        if not self._ensure_client():
            return None

        try:
            run_info = self.client.jobs.get_run(run_id=run_id)
            logs = []

            # Strategy 1: Get task-level logs via get_run_output (most reliable)
            if hasattr(run_info, 'tasks') and run_info.tasks:
                task_logs = self._get_task_logs(run_info.tasks)
                if task_logs:
                    logs.append("=== TASK EXECUTION LOGS ===\n" + task_logs)
            else:
                # Single-task job
                single_task_logs = self._get_single_task_logs(run_id)
                if single_task_logs:
                    logs.append("=== JOB EXECUTION LOGS ===\n" + single_task_logs)

            if logs:
                return "\n\n".join(logs)
            else:
                return self._generate_no_logs_message(run_info)

        except Exception as e:
            show_logs_load_error(run_id, f"Failed to retrieve logs: {str(e)}")
            return None

    def _get_task_logs(self, tasks) -> str:
        """Retrieve logs for multi-task jobs."""
        logs = []
        for task in tasks:
            task_key = task.task_key if hasattr(task, 'task_key') else 'unknown'
            task_run_id = task.run_id if hasattr(task, 'run_id') else None

            if task_run_id:
                try:
                    task_output = self.client.jobs.get_run_output(run_id=task_run_id)
                    task_logs = self._extract_logs_from_output(task_output)

                    if task_logs:
                        logs.append(f"--- Task: {task_key} (Run ID: {task_run_id}) ---\n{task_logs}")
                    else:
                        logs.append(f"--- Task: {task_key} (Run ID: {task_run_id}) ---\nNo output logs available")

                except Exception as task_error:
                    logs.append(f"--- Task: {task_key} (Run ID: {task_run_id}) ---\nError retrieving logs: {str(task_error)}")

        return "\n\n".join(logs) if logs else ""

    def _get_single_task_logs(self, run_id: int) -> str:
        """Retrieve logs for single-task jobs."""
        try:
            output = self.client.jobs.get_run_output(run_id=run_id)
            return self._extract_logs_from_output(output)
        except Exception as e:
            error_msg = str(e)
            if "multiple tasks" in error_msg.lower():
                return "This job has multiple tasks. Logs should be retrieved per task."
            else:
                return f"Could not retrieve logs: {error_msg}"

    def _extract_logs_from_output(self, output) -> str:
        """Extract log content from job output object, prioritizing stderr."""
        logs = ""
        if hasattr(output, 'stderr') and output.stderr:
            stderr_filtered = self._filter_migration_logs(output.stderr)
            if stderr_filtered:
                logs += "=== STDERR ===\n" + stderr_filtered + "\n"
        if hasattr(output, 'logs') and output.logs:
            stdout_filtered = self._filter_migration_logs(output.logs)
            if stdout_filtered:
                logs += "=== STDOUT ===\n" + stdout_filtered + "\n"
        if logs:
            return logs.strip()
        elif hasattr(output, 'error') and output.error:
            error_msg = output.error
            if "No output is available until the task begins" in error_msg:
                return "Task is pending or starting. Logs will be available once execution begins."
            return f"Task Error: {error_msg}"
        elif hasattr(output, 'error_trace') and output.error_trace:
            return f"Task Error Trace:\n{output.error_trace}"
        elif hasattr(output, 'result') and output.result:
            # Handle notebook results or other structured outputs
            return f"Task Result: {str(output.result)}"
        else:
            return ""

    def _filter_migration_logs(self, logs_str: str) -> str:
        """Filter logs to only include lines containing 'MIGRATION_ACCELERATOR'."""
        if not logs_str:
            return ""
        lines = logs_str.splitlines()
        filtered_lines = [line for line in lines if 'MIGRATION_ACCELERATOR' in line]
        return '\n'.join(filtered_lines)

    def _get_cluster_logs(self, run_info) -> str:
        """Attempt to retrieve cluster-level events."""
        try:
            cluster_id = self._extract_cluster_id_from_run(run_info)

            if not cluster_id:
                return "No cluster ID found in run information. Cluster events may not be available."

            logs = []
            try:
                events = list(self.client.clusters.events(cluster_id=cluster_id, limit=50))
                if events:
                    event_logs = []
                    for event in events[-10:]:
                        timestamp = event.timestamp if hasattr(event, 'timestamp') else 'Unknown'
                        event_type = event.type if hasattr(event, 'type') else 'Unknown'
                        details = event.details if hasattr(event, 'details') else 'No details'
                        event_logs.append(f"[{timestamp}] {event_type}: {details}")
                    logs.append("Recent cluster events:\n" + "\n".join(event_logs))
                else:
                    logs.append("No cluster events available (cluster may not have event logging enabled).")
            except Exception as e:
                logs.append(f"Could not retrieve cluster events: {str(e)}")

            return "\n\n".join(logs) if logs else ""

        except Exception as e:
            return f"Error retrieving cluster logs: {str(e)}"

    def _generate_no_logs_message(self, run_info) -> str:
        """Generate informative message when no logs are found."""
        messages = ["No logs could be retrieved for this run."]

        if hasattr(run_info, 'state') and run_info.state:
            lifecycle = None
            result = None

            if hasattr(run_info.state, 'life_cycle_state'):
                lifecycle = str(run_info.state.life_cycle_state.value) if hasattr(run_info.state.life_cycle_state, 'value') else str(run_info.state.life_cycle_state)

            if hasattr(run_info.state, 'result_state'):
                result = str(run_info.state.result_state.value) if hasattr(run_info.state.result_state, 'result_state') else str(run_info.state.result_state)

            if lifecycle:
                messages.append(f"Run state: {lifecycle}")
                if result:
                    messages.append(f"Result: {result}")

                if lifecycle in ['PENDING', 'RUNNING']:
                    messages.append("Run is still in progress. Logs may not be available yet.")
                elif lifecycle == 'TERMINATED' and result == 'SUCCESS':
                    messages.append("Run completed successfully but logs were not captured.")
                elif result == 'FAILED':
                    messages.append("Run failed. Check job configuration and permissions.")

        messages.append("\nTroubleshooting tips:")
        messages.append("1. Ensure your job outputs logs to standard output (stdout/stderr).")
        messages.append("2. Check that your workspace has permissions to access job run output.")
        messages.append("3. Wait a few moments if run just completed.")

        return "\n".join(messages)

    def get_cluster_logs_by_id(self, cluster_id: str) -> str:
        """
        Retrieve logs for a specific cluster by ID.
        This method is simplified to only retrieve cluster events.

        Args:
            cluster_id: The cluster ID to retrieve logs for

        Returns:
            Log content as string
        """
        if not self._ensure_client():
            return "No valid Databricks connection"

        if not cluster_id:
            return "No cluster ID provided"

        logs = []

        try:
            events = list(self.client.clusters.events(cluster_id=cluster_id, limit=50))
            if events:
                event_logs = []
                for event in events[-10:]:
                    timestamp = event.timestamp if hasattr(event, 'timestamp') else 'Unknown'
                    event_type = event.type if hasattr(event, 'type') else 'Unknown'
                    details = event.details if hasattr(event, 'details') else 'No details'
                    event_logs.append(f"[{timestamp}] {event_type}: {details}")
                logs.append("Recent cluster events:\n" + "\n".join(event_logs))
            else:
                logs.append("No cluster events available (cluster may not have event logging enabled).")
        except Exception as e:
            logs.append(f"Could not retrieve cluster events: {str(e)}")

        return "\n\n".join(logs) if logs else "No cluster logs could be retrieved."

    def get_cluster_logs_for_run(self, run_id: int) -> str:
        """
        Retrieve cluster logs for a specific job run by extracting cluster ID from run metadata.
        This method is simplified to only retrieve cluster events.

        Args:
            run_id: The run ID to get cluster logs for

        Returns:
            Log content as string
        """
        if not self._ensure_client():
            return "No valid Databricks connection"

        try:
            run_info = self.client.jobs.get_run(run_id=run_id)
            cluster_id = self._extract_cluster_id_from_run(run_info)

            if not cluster_id:
                return "Could not determine cluster ID from run information. This may be a job cluster that has already been terminated."

            return self.get_cluster_logs_by_id(cluster_id)

        except Exception as e:
            return f"Failed to retrieve cluster logs for run {run_id}: {str(e)}"

    def _extract_cluster_id_from_run(self, run_info) -> Optional[str]:
        """
        Extract cluster ID from job run information.

        Args:
            run_info: The run information object

        Returns:
            Cluster ID as string or None if not found
        """
        if hasattr(run_info, 'cluster_instance') and run_info.cluster_instance:
            if hasattr(run_info.cluster_instance, 'cluster_id'):
                return run_info.cluster_instance.cluster_id

        if hasattr(run_info, 'cluster_spec') and run_info.cluster_spec:
            if hasattr(run_info.cluster_spec, 'cluster_id'):
                return run_info.cluster_spec.cluster_id

        if hasattr(run_info, 'job_clusters') and run_info.job_clusters:
            for job_cluster in run_info.job_clusters:
                if hasattr(job_cluster, 'cluster_id'):
                    return job_cluster.cluster_id
                if hasattr(job_cluster, 'new_cluster') and job_cluster.new_cluster:
                    if hasattr(job_cluster.new_cluster, 'cluster_id'):
                        return job_cluster.new_cluster.cluster_id

        if hasattr(run_info, 'tasks') and run_info.tasks:
            for task in run_info.tasks:
                if hasattr(task, 'cluster_instance') and task.cluster_instance:
                    if hasattr(task.cluster_instance, 'cluster_id'):
                        return task.cluster_instance.cluster_id

        return None

    def _get_cluster_id_from_job_cluster_key(self, run_info, job_cluster_key: str) -> Optional[str]:
        """
        Get cluster ID from job cluster key by looking up the job specification.

        Args:
            run_info: The run information object
            job_cluster_key: The job cluster key reference

        Returns:
            Cluster ID as string or None if not found
        """
        try:
            if hasattr(run_info, 'job_id'):
                job_details = self.client.jobs.get(job_id=run_info.job_id)
                if hasattr(job_details, 'settings') and job_details.settings:
                    settings = job_details.settings
                    if hasattr(settings, 'job_clusters') and settings.job_clusters:
                        for job_cluster in settings.job_clusters:
                            if hasattr(job_cluster, 'job_cluster_key') and job_cluster.job_cluster_key == job_cluster_key:
                                if hasattr(job_cluster, 'cluster_id') and job_cluster.cluster_id:
                                    return job_cluster.cluster_id
        except Exception:
            pass
        return None


# Global job manager instance
_job_manager = None
_last_host = None
_last_token = None


def get_job_manager() -> JobManager:
    """Get the global job manager instance, recreating if connection changed."""
    global _job_manager, _last_host, _last_token
    
    current_host = st.session_state.get('databricks_host', '')
    current_token = st.session_state.get('databricks_token', '')
    
    if (_job_manager is None or
        _last_host != current_host or
        _last_token != current_token):
        _job_manager = JobManager()
        _last_host = current_host
        _last_token = current_token
    
    return _job_manager