"""
Error handling with retry mechanism for the Migration Accelerator.

Uses Decorator pattern for automatic error handling and retries.
"""

import time
from typing import Callable, Any, Optional, Dict
from functools import wraps
from enum import Enum

from .logger import get_logger, LogLevel


class ErrorSeverity(Enum):
    """Error severity levels."""
    FATAL = "fatal"
    RECOVERABLE = "recoverable"
    WARNING = "warning"


class AcceleratorError(Exception):
    """Base exception for Migration Accelerator errors."""
    
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.FATAL,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None
    ):
        """Initialize error."""
        super().__init__(message)
        self.message = message
        self.severity = severity
        self.context = context or {}
        self.original_error = original_error


def handle_node_error(
    node_name: str,
    context: Optional[Dict[str, Any]] = None
):
    """
    Decorator to handle errors in pipeline nodes.
    
    Args:
        node_name: Name of the node
        context: Additional context for error logging
    
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger(node_name)
            error_context = context or {}
            
            try:
                logger.start_stage(node_name, error_context)
                result = func(*args, **kwargs)
                logger.end_stage(node_name, success=True, context=error_context)
                return result
            except AcceleratorError as e:
                logger.error(
                    f"Error in {node_name}: {e.message}",
                    context={**error_context, **e.context},
                    error=str(e.original_error) if e.original_error else None
                )
                logger.end_stage(node_name, success=False, context=error_context)
                raise
            except Exception as e:
                logger.error(
                    f"Unexpected error in {node_name}: {str(e)}",
                    context=error_context,
                    error=str(e)
                )
                logger.end_stage(node_name, success=False, context=error_context)
                raise AcceleratorError(
                    f"Unexpected error in {node_name}: {str(e)}",
                    severity=ErrorSeverity.FATAL,
                    context=error_context,
                    original_error=e
                )
        
        return wrapper
    return decorator


def retry_on_error(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0,
    logger_name: str = "error_handler"
):
    """
    Decorator to retry function on errors.
    
    Args:
        max_retries: Maximum number of retries
        retry_delay: Initial delay between retries (seconds)
        backoff_factor: Multiplier for retry delay
        logger_name: Logger name for retry messages
    
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger(logger_name)
            delay = retry_delay
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        logger.warning(
                            f"Retrying {func.__name__} (attempt {attempt + 1}/{max_retries + 1})",
                            context={"function": func.__name__, "attempt": attempt + 1}
                        )
                        time.sleep(delay)
                        delay *= backoff_factor
                    
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries:
                        logger.warning(
                            f"Error in {func.__name__}: {str(e)}",
                            context={"function": func.__name__, "attempt": attempt + 1, "error": str(e)}
                        )
                    else:
                        logger.error(
                            f"Failed {func.__name__} after {max_retries + 1} attempts",
                            context={"function": func.__name__, "attempts": max_retries + 1},
                            error=str(e)
                        )
                        raise
        
        return wrapper
    return decorator

