"""
Job run data parsing utilities for Databricks Job Executor.

Extracts and transforms run data from Databricks API responses.
"""
from typing import Dict, Any, Optional


def parse_run_state(run) -> Dict[str, Optional[str]]:
    """
    Parse lifecycle and result state from a run object.
    
    Args:
        run: Databricks run object
        
    Returns:
        Dictionary with 'lifecycle_state' and 'result_state'
    """
    lifecycle_state = None
    result_state = None
    
    if hasattr(run, 'state') and run.state:
        if hasattr(run.state, 'life_cycle_state') and run.state.life_cycle_state:
            lifecycle_state = str(run.state.life_cycle_state.value) if hasattr(
                run.state.life_cycle_state, 'value'
            ) else str(run.state.life_cycle_state)
        
        if hasattr(run.state, 'result_state') and run.state.result_state:
            result_state = str(run.state.result_state.value) if hasattr(
                run.state.result_state, 'value'
            ) else str(run.state.result_state)
    
    return {
        'lifecycle_state': lifecycle_state,
        'result_state': result_state
    }


def calculate_duration(run) -> Optional[int]:
    """
    Calculate execution duration from run object.
    
    Tries execution_duration field first, then calculates from timestamps.
    
    Args:
        run: Databricks run object
        
    Returns:
        Duration in milliseconds, or None if unavailable
    """
    # Try direct execution_duration field
    if hasattr(run, 'execution_duration') and run.execution_duration:
        return run.execution_duration
    
    # Calculate from timestamps
    start_time = run.start_time if hasattr(run, 'start_time') else None
    end_time = run.end_time if hasattr(run, 'end_time') else None
    
    if start_time and end_time:
        return end_time - start_time
    
    return None


def extract_run_metadata(run, job_id: int) -> Dict[str, Any]:
    """
    Extract standardized metadata from a run object.
    
    Args:
        run: Databricks run object
        job_id: Job ID associated with the run
        
    Returns:
        Dictionary with standardized run metadata
    """
    run_id = run.run_id if hasattr(run, 'run_id') else None
    
    # Parse state
    state = parse_run_state(run)
    lifecycle_state = state['lifecycle_state']
    result_state = state['result_state']
    
    # Determine display status
    if lifecycle_state == 'TERMINATED':
        status = result_state or 'COMPLETED'
    elif lifecycle_state:
        status = lifecycle_state
    else:
        status = 'UNKNOWN'
    
    # Get timestamps
    start_time_ms = run.start_time if hasattr(run, 'start_time') else None
    end_time_ms = run.end_time if hasattr(run, 'end_time') else None
    
    # Calculate duration
    duration_ms = calculate_duration(run)
    
    # Get job name
    job_name = f"Job {job_id}"
    if hasattr(run, 'run_name') and run.run_name:
        job_name = run.run_name
    
    return {
        'run_id': run_id,
        'job_id': job_id,
        'job_name': job_name,
        'status': status,
        'lifecycle_state': lifecycle_state,
        'result_state': result_state,
        'start_time_ms': start_time_ms,
        'end_time_ms': end_time_ms,
        'execution_duration': duration_ms,
    }


def get_display_status(lifecycle_state: Optional[str], result_state: Optional[str]) -> str:
    """
    Get user-friendly status from lifecycle and result states.
    
    Args:
        lifecycle_state: Lifecycle state from Databricks
        result_state: Result state from Databricks
        
    Returns:
        Display-friendly status string
    """
    if lifecycle_state == 'TERMINATED':
        if result_state == 'SUCCESS':
            return 'SUCCESS'
        elif result_state == 'FAILED':
            return 'FAILED'
        elif result_state in ['CANCELLED', 'CANCELED']:
            return 'CANCELLED'
        else:
            return 'TERMINATED'
    elif lifecycle_state == 'RUNNING':
        return 'RUNNING'
    elif lifecycle_state == 'PENDING':
        return 'PENDING'
    else:
        return lifecycle_state or 'UNKNOWN'
