"""
Error handling utilities for UI components.
"""
import streamlit as st


def handle_api_error(operation: str, error_message: str):
    """
    Handle API errors and display them to the user.

    Args:
        operation: Description of the operation that failed
        error_message: Error message to display
    """
    st.error(f"Error during {operation}: {error_message}")


def handle_job_execution_error(job_id: int, error_message: str):
    """
    Handle job execution errors.

    Args:
        job_id: The job ID that failed
        error_message: Error message to display
    """
    st.error(f"Failed to execute job {job_id}: {error_message}")


def handle_job_cancellation_error(run_id: int, error_message: str):
    """
    Handle job cancellation errors.

    Args:
        run_id: The run ID that failed to cancel
        error_message: Error message to display
    """
    st.error(f"Failed to cancel run {run_id}: {error_message}")


def show_logs_load_error(run_id: int, error_message: str):
    """
    Show error when logs fail to load.

    Args:
        run_id: The run ID whose logs failed to load
        error_message: Error message to display
    """
    st.warning(f"Could not load logs for run {run_id}: {error_message}")


