"""
Formatting utilities for display in the Databricks Job Executor UI.
"""
import time
from typing import Optional


def format_duration(duration_ms: Optional[int]) -> str:
    """
    Format duration in milliseconds to human-readable string.
    
    Args:
        duration_ms: Duration in milliseconds
        
    Returns:
        Human-readable duration string (e.g., "2m 30s")
    """
    if not duration_ms or duration_ms < 0:
        return 'N/A'
    
    total_seconds = duration_ms // 1000
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def format_timestamp(timestamp_ms: Optional[int]) -> str:
    """
    Format timestamp in milliseconds to human-readable datetime string.
    
    Args:
        timestamp_ms: Timestamp in milliseconds since epoch
        
    Returns:
        Formatted datetime string (e.g., "2026-01-12 10:30:45")
    """
    if not timestamp_ms:
        return 'N/A'
    
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp_ms / 1000))


def get_status_emoji(status: str) -> str:
    """
    Get emoji representation for job status.
    
    Args:
        status: Job status string
        
    Returns:
        Emoji string representing the status
    """
    status_upper = status.upper()
    
    if status_upper == 'SUCCESS':
        return '✅'
    elif status_upper == 'FAILED':
        return '❌'
    elif status_upper in ['CANCELLED', 'CANCELED']:
        return '⚠️'
    elif status_upper == 'RUNNING':
        return '🏃'
    elif status_upper == 'PENDING':
        return '⏳'
    else:
        return '🔘'
