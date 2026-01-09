"""Result merging utilities for translation outputs.

Provides functions to merge and aggregate translation results from multiple
batch runs into a single consolidated result structure.
"""

from typing import Dict, Any, List


# Default artifact types for result structure
ARTIFACT_TYPES = [
    "databases", "schemas", "tables", "views", "stages", "external_locations",
    "streams", "pipes", "roles", "grants", "tags", "comments",
    "masking_policies", "udfs", "procedures"
]


def create_empty_result() -> Dict[str, Any]:
    """
    Create an empty result structure for merging.
    
    Returns:
        Dictionary with empty result structure ready for merging
    """
    result = {artifact_type: [] for artifact_type in ARTIFACT_TYPES}
    result["metadata"] = {
        "total_results": 0,
        "errors": [],
        "processing_stats": {}
    }
    return result


def accumulate_processing_stats(
    target_stats: Dict[str, Any],
    source_stats: Dict[str, Any]
) -> None:
    """
    Accumulate processing stats from source into target.
    
    Args:
        target_stats: Target stats dictionary to update
        source_stats: Source stats dictionary to merge from
    """
    for artifact_type, stats in source_stats.items():
        if artifact_type in target_stats:
            existing = target_stats[artifact_type]
            existing["count"] = existing.get("count", 0) + stats.get("count", 0)
            existing["errors"] = existing.get("errors", 0) + stats.get("errors", 0)
            existing["processed"] = existing.get("processed", 0) + stats.get("processed", 0)
        else:
            target_stats[artifact_type] = stats.copy()


def merge_metadata(
    merged_metadata: Dict[str, Any],
    source_metadata: Dict[str, Any]
) -> None:
    """
    Merge metadata from a single result into merged metadata.
    
    Args:
        merged_metadata: Target metadata dictionary to update
        source_metadata: Source metadata dictionary to merge from
    """
    merged_metadata["total_results"] += source_metadata.get("total_results", 0)
    merged_metadata["errors"].extend(source_metadata.get("errors", []))
    accumulate_processing_stats(
        merged_metadata["processing_stats"],
        source_metadata.get("processing_stats", {})
    )


def merge_observability_stages(
    target_stages: Dict[str, Any],
    source_stages: Dict[str, Any]
) -> None:
    """
    Merge stage metrics from source into target.
    
    Args:
        target_stages: Target stages dictionary to update
        source_stages: Source stages dictionary to merge from
    """
    for stage_name, stage_data in source_stages.items():
        if stage_name not in target_stages:
            target_stages[stage_name] = stage_data.copy()
        else:
            target_stages[stage_name]["items_processed"] += stage_data.get("items_processed", 0)
            target_stages[stage_name]["error_count"] += stage_data.get("error_count", 0)
            if "duration" in stage_data and stage_data["duration"] is not None:
                current = target_stages[stage_name].get("duration", 0) or 0
                target_stages[stage_name]["duration"] = current + stage_data["duration"]


def merge_ai_metrics(
    target_metrics: Dict[str, Any],
    source_metrics: Dict[str, Any]
) -> None:
    """
    Merge AI metrics from source into target.
    
    Args:
        target_metrics: Target AI metrics dictionary to update
        source_metrics: Source AI metrics dictionary to merge from
    """
    for ai_key, ai_data in source_metrics.items():
        if ai_key not in target_metrics:
            target_metrics[ai_key] = ai_data.copy()
        else:
            target_metrics[ai_key]["call_count"] += ai_data.get("call_count", 0)
            target_metrics[ai_key]["total_latency"] += ai_data.get("total_latency", 0)
            target_metrics[ai_key]["errors"] += ai_data.get("errors", 0)
            if target_metrics[ai_key]["call_count"] > 0:
                target_metrics[ai_key]["average_latency"] = (
                    target_metrics[ai_key]["total_latency"] /
                    target_metrics[ai_key]["call_count"]
                )


def merge_observability(
    merged_result: Dict[str, Any],
    source_obs: Dict[str, Any]
) -> None:
    """
    Merge observability data from a single result into merged result.
    
    Args:
        merged_result: Target result dictionary containing observability
        source_obs: Source observability dictionary to merge from
    """
    if "observability" not in merged_result:
        merged_result["observability"] = {
            "run_id": source_obs.get("run_id"),
            "total_duration": 0,
            "total_errors": 0,
            "total_warnings": 0,
            "total_retries": 0,
            "artifact_counts": {},
            "stages": {},
            "ai_metrics": {}
        }
    
    obs = merged_result["observability"]
    
    # Aggregate artifact_counts
    for artifact_type, count in source_obs.get("artifact_counts", {}).items():
        obs["artifact_counts"][artifact_type] = obs["artifact_counts"].get(artifact_type, 0) + count
    
    # Aggregate totals
    obs["total_errors"] += source_obs.get("total_errors", 0)
    obs["total_warnings"] += source_obs.get("total_warnings", 0)
    obs["total_retries"] += source_obs.get("total_retries", 0)
    
    # Merge nested data
    merge_observability_stages(obs["stages"], source_obs.get("stages", {}))
    merge_ai_metrics(obs["ai_metrics"], source_obs.get("ai_metrics", {}))


def merge_result_into(
    merged_result: Dict[str, Any],
    result: Dict[str, Any]
) -> None:
    """
    Merge a single result into the merged result structure.
    
    Args:
        merged_result: The merged result dictionary to update
        result: A single result dictionary to merge
    """
    for key, value in result.items():
        if key == "metadata":
            merge_metadata(merged_result["metadata"], result["metadata"])
        elif key == "observability":
            merge_observability(merged_result, value)
        elif key in merged_result:
            merged_result[key].extend(value)


def merge_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple translation results into a single consolidated result.
    
    This is a convenience function that creates an empty result and merges
    all provided results into it.
    
    Args:
        results: List of result dictionaries to merge
        
    Returns:
        Merged result dictionary
    """
    merged = create_empty_result()
    for result in results:
        merge_result_into(merged, result)
    return merged
