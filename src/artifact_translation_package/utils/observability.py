"""
Observability facade for the Migration Accelerator.

Provides a unified interface for logging, error handling, and metrics.
Uses Facade pattern to simplify observability operations.
"""

from typing import Dict, Any, Optional
from datetime import datetime

from .logger import get_logger, Logger, LogLevel, FileLogHandler
from .metrics import get_metrics_collector, MetricsCollector
from .error_handler import handle_node_error, retry_on_error


class Observability:
    """
    Facade for observability operations.
    
    Provides a single entry point for logging, metrics, and error handling.
    """
    
    def __init__(
        self,
        run_id: Optional[str] = None,
        log_level: LogLevel = LogLevel.INFO,
        log_file: Optional[str] = None
    ):
        """
        Initialize observability.
        
        Args:
            run_id: Unique identifier for this run
            log_level: Minimum log level
            log_file: Optional path to log file
        """
        self.run_id = run_id or f"run_{int(datetime.utcnow().timestamp())}"
        self.log_level = log_level
        
        # Setup logger
        handlers = []
        if log_file:
            handlers.append(FileLogHandler(log_file))
        
        self.logger = get_logger("observability", level=log_level, handlers=handlers)
        
        # Setup metrics - reset to clear any accumulated state from previous runs
        self.metrics = get_metrics_collector()
        self.metrics.reset()
        self.metrics.set_run_id(self.run_id)
        
        self.logger.info("Observability initialized", context={"run_id": self.run_id})
    
    def get_logger(self, name: str) -> Logger:
        """Get a logger for a component."""
        return get_logger(name, level=self.log_level)
    
    def get_metrics(self) -> MetricsCollector:
        """Get metrics collector."""
        return self.metrics
    
    def finalize(self) -> Dict[str, Any]:
        """
        Finalize observability and generate summary.
        
        Returns:
            Summary dictionary with metrics and status
        """
        self.metrics.complete_run()
        summary = self.metrics.get_summary()
        
        self.logger.info("Observability finalized", context={
            "run_id": self.run_id,
            "total_duration": summary["total_duration"],
            "total_errors": summary["total_errors"]
        })
        
        return summary


# Global observability instance
_observability: Optional[Observability] = None


def initialize(
    run_id: Optional[str] = None,
    log_level: LogLevel = LogLevel.INFO,
    log_file: Optional[str] = None
) -> Observability:
    """
    Initialize global observability instance.
    
    Args:
        run_id: Unique identifier for this run
        log_level: Minimum log level
        log_file: Optional path to log file
    
    Returns:
        Observability instance
    """
    global _observability
    _observability = Observability(run_id=run_id, log_level=log_level, log_file=log_file)
    return _observability


def get_observability() -> Optional[Observability]:
    """Get the global observability instance."""
    return _observability


def finalize() -> Dict[str, Any]:
    """Finalize global observability."""
    if _observability:
        return _observability.finalize()
    return {}
