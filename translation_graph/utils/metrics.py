"""
Performance metrics collection for the Migration Accelerator.

Uses Singleton pattern for global metrics tracking.
"""

import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from .logger import get_logger


@dataclass
class StageMetrics:
    """Metrics for a single stage."""
    stage_name: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    success: bool = True
    items_processed: int = 0
    error_count: int = 0
    context: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, success: bool = True):
        """Mark stage as complete."""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.success = success


@dataclass
class AIMetrics:
    """Metrics for AI/LLM usage."""
    provider: str
    model: str
    call_count: int = 0
    total_latency: float = 0.0
    errors: int = 0
    
    def add_call(self, latency: float, error: bool = False):
        """Record an AI call."""
        self.call_count += 1
        self.total_latency += latency
        if error:
            self.errors += 1


class MetricsCollector:
    """
    Metrics collector (Singleton pattern).
    
    Tracks performance metrics across the pipeline.
    """
    
    _instance: Optional['MetricsCollector'] = None
    
    def __new__(cls):
        """Ensure only one instance exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize metrics collector."""
        if self._initialized:
            return
        
        self.run_id: Optional[str] = None
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None
        
        self.stages: Dict[str, StageMetrics] = {}
        self.ai_metrics: Dict[str, AIMetrics] = {}
        self.artifact_counts: Dict[str, int] = defaultdict(int)
        
        self.total_errors: int = 0
        self.total_warnings: int = 0
        self.total_retries: int = 0
        
        self.logger = get_logger("metrics")
        self._initialized = True
    
    def set_run_id(self, run_id: str):
        """Set run ID for this execution."""
        self.run_id = run_id
    
    def start_stage(self, stage_name: str, context: Optional[Dict[str, Any]] = None) -> StageMetrics:
        """Start tracking a stage."""
        metrics = StageMetrics(
            stage_name=stage_name,
            start_time=time.time(),
            context=context or {}
        )
        self.stages[stage_name] = metrics
        return metrics
    
    def end_stage(self, stage_name: str, success: bool = True, items_processed: int = 0):
        """End tracking a stage."""
        if stage_name in self.stages:
            self.stages[stage_name].complete(success)
            self.stages[stage_name].items_processed = items_processed
            if not success:
                self.stages[stage_name].error_count += 1
                self.total_errors += 1
    
    def record_artifact(self, artifact_type: str, count: int = 1):
        """Record processed artifacts."""
        self.artifact_counts[artifact_type] += count
    
    def record_ai_call(self, provider: str, model: str, latency: float, error: bool = False):
        """Record an AI/LLM call."""
        key = f"{provider}:{model}"
        if key not in self.ai_metrics:
            self.ai_metrics[key] = AIMetrics(provider=provider, model=model)
        self.ai_metrics[key].add_call(latency, error)
    
    def record_warning(self):
        """Record a warning."""
        self.total_warnings += 1
    
    def record_retry(self):
        """Record a retry."""
        self.total_retries += 1
    
    def complete_run(self):
        """Mark run as complete."""
        self.end_time = time.time()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics."""
        total_duration = (self.end_time or time.time()) - self.start_time
        
        stage_summaries = {
            name: {
                "duration": metrics.duration,
                "success": metrics.success,
                "items_processed": metrics.items_processed,
                "error_count": metrics.error_count
            }
            for name, metrics in self.stages.items()
        }
        
        ai_summaries = {
            key: {
                "call_count": metrics.call_count,
                "total_latency": metrics.total_latency,
                "average_latency": metrics.total_latency / metrics.call_count if metrics.call_count > 0 else 0.0,
                "errors": metrics.errors
            }
            for key, metrics in self.ai_metrics.items()
        }
        
        return {
            "run_id": self.run_id,
            "total_duration": total_duration,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "total_retries": self.total_retries,
            "artifact_counts": dict(self.artifact_counts),
            "stages": stage_summaries,
            "ai_metrics": ai_summaries
        }


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    return MetricsCollector()

