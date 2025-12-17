"""
Helper functions for observability integration.

Simplifies adding observability to translation nodes.
"""

import time
from typing import Dict, Any, Optional
from functools import wraps

from .observability import get_observability


def track_ai_call(provider: str, model: str, func):
    """
    Decorator to track AI/LLM calls with metrics.
    
    Args:
        provider: AI provider name
        model: Model name
        func: Function that makes the AI call
    
    Returns:
        Wrapped function with AI call tracking
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        obs = get_observability()
        metrics = obs.get_metrics() if obs else None
        
        start_time = time.time()
        error_occurred = False
        
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            error_occurred = True
            raise
        finally:
            if metrics:
                latency = time.time() - start_time
                metrics.record_ai_call(provider, model, latency, error_occurred)
    
    return wrapper


def with_observability(node_name: str, artifact_type: str):
    """
    Decorator factory for adding observability to translation nodes.
    
    Args:
        node_name: Name of the node
        artifact_type: Type of artifact being processed
    
    Returns:
        Decorator function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(batch, *args, **kwargs):
            obs = get_observability()
            logger = obs.get_logger(node_name) if obs else None
            metrics = obs.get_metrics() if obs else None
            
            context = {
                "artifact_type": batch.artifact_type,
                "batch_size": len(batch.items)
            }
            
            if metrics:
                metrics.start_stage(node_name, context)
            
            try:
                result = func(batch, *args, **kwargs)
                
                if metrics:
                    metrics.end_stage(node_name, success=True, items_processed=len(batch.items))
                    metrics.record_artifact(artifact_type, count=len(batch.items))
                
                return result
            except Exception as e:
                if metrics:
                    metrics.end_stage(node_name, success=False)
                raise
        
        return wrapper
    return decorator
