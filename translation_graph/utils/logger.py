"""
Structured logging for the Migration Accelerator.

Uses Strategy pattern for different log output backends.
"""

import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod
from enum import Enum


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40


class LogHandler(ABC):
    """Abstract base class for log handlers (Strategy pattern)."""
    
    @abstractmethod
    def handle(self, level: LogLevel, message: str, context: Dict[str, Any]) -> None:
        """Handle a log entry."""
        pass


class ConsoleLogHandler(LogHandler):
    """Console log handler."""
    
    def handle(self, level: LogLevel, message: str, context: Dict[str, Any]) -> None:
        """Write log to console."""
        timestamp = datetime.utcnow().isoformat() + "Z"
        level_name = level.name
        # Ensure we can clearly identify logs in cluster output
        app_tag = "MIGRATION_ACCELERATOR"
        component = None
        if isinstance(context, dict):
            component = context.get("component")
        context_str = json.dumps(context) if context else "{}"
        if component:
            prefix = f"[{timestamp}] [{app_tag}] [{component}] {level_name}"
        else:
            prefix = f"[{timestamp}] [{app_tag}] {level_name}"
        log_line = f"{prefix} - {message} | Context: {context_str}\n"
        sys.stdout.write(log_line)
        sys.stdout.flush()


class FileLogHandler(LogHandler):
    """File log handler."""
    
    def __init__(self, file_path: str):
        """Initialize file handler."""
        self.file_path = file_path
        self._file = None
    
    def handle(self, level: LogLevel, message: str, context: Dict[str, Any]) -> None:
        """Write log to file."""
        if self._file is None:
            self._file = open(self.file_path, 'a', encoding='utf-8')
        
        timestamp = datetime.utcnow().isoformat() + "Z"
        level_name = level.name
        context_str = json.dumps(context) if context else "{}"
        log_entry = {
            "timestamp": timestamp,
            "level": level_name,
            "message": message,
            "context": context
        }
        self._file.write(json.dumps(log_entry) + "\n")
        self._file.flush()
    
    def close(self):
        """Close the file."""
        if self._file:
            self._file.close()
            self._file = None


class Logger:
    """
    Structured logger with multiple output handlers.
    
    Uses Strategy pattern for different log backends.
    """
    
    def __init__(self, name: str, level: LogLevel = LogLevel.INFO, handlers: Optional[List[LogHandler]] = None):
        """
        Initialize logger.
        
        Args:
            name: Logger name (component/stage name)
            level: Minimum log level
            handlers: List of log handlers (defaults to console)
        """
        self.name = name
        self.level = level
        self.handlers = handlers or [ConsoleLogHandler()]
    
    def _log(self, level: LogLevel, message: str, context: Optional[Dict[str, Any]] = None):
        """Internal logging method."""
        if level.value < self.level.value:
            return
        
        context = context or {}
        context["component"] = self.name
        
        for handler in self.handlers:
            handler.handle(level, message, context)
    
    def debug(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, context)
    
    def info(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Log info message."""
        self._log(LogLevel.INFO, message, context)
    
    def warning(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Log warning message."""
        self._log(LogLevel.WARNING, message, context)
    
    def error(self, message: str, context: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        """Log error message."""
        context = context or {}
        if error:
            context["error"] = error
        self._log(LogLevel.ERROR, message, context)
    
    def start_stage(self, stage_name: str, context: Optional[Dict[str, Any]] = None):
        """Log stage start."""
        context = context or {}
        context["stage"] = stage_name
        context["status"] = "start"
        self.info(f"Starting {stage_name}", context)
    
    def end_stage(self, stage_name: str, success: bool = True, context: Optional[Dict[str, Any]] = None):
        """Log stage end."""
        context = context or {}
        context["stage"] = stage_name
        context["status"] = "success" if success else "failure"
        self.info(f"Completed {stage_name}", context)


# Global logger registry (Singleton pattern)
_loggers: Dict[str, Logger] = {}


def get_logger(name: str, level: Optional[LogLevel] = None, handlers: Optional[List[LogHandler]] = None) -> Logger:
    """
    Get or create a logger instance.
    
    Args:
        name: Logger name
        level: Log level (uses default if not provided)
        handlers: Custom handlers (uses default if not provided)
    
    Returns:
        Logger instance
    """
    if name not in _loggers:
        _loggers[name] = Logger(name, level or LogLevel.INFO, handlers)
    return _loggers[name]

