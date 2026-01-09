from typing import List, Dict, Any, Optional
from artifact_translation_package.utils.types import TranslationResult
from artifact_translation_package.utils.error_handler import handle_node_error
from artifact_translation_package.utils.observability import get_observability


@handle_node_error("aggregate_translations")
def aggregate_translations(*results: TranslationResult, evaluation_results: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Aggregate multiple translation results into a final merged structure.

    Args:
        *results: Variable number of TranslationResult objects
        evaluation_results: Optional list of evaluation result dictionaries

    Returns:
        Dictionary with merged results including all artifact types
    """
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    
    if metrics:
        metrics.start_stage("aggregate_translations", {"result_count": len(results)})
    
    try:
        all_artifact_types = {
            "databases", "schemas", "tables", "views", "stages", "external_locations",
            "streams", "pipes", "roles", "grants", "tags", "comments",
            "masking_policies", "udfs", "procedures", "sequences"
        }
        
        merged = {artifact_type: [] for artifact_type in all_artifact_types}
        merged["metadata"] = {
            "total_results": 0,
            "errors": [],
            "processing_stats": {},
            "evaluation_results_count": len(evaluation_results) if evaluation_results else 0
        }

        for result in results:
            artifact_type = result.artifact_type
            if artifact_type in merged:
                merged[artifact_type].extend(result.results)
            else:
                merged[artifact_type] = result.results

            # Collect errors
            if result.errors:
                merged["metadata"]["errors"].extend(result.errors)

            # Update processing stats - accumulate if artifact_type already exists
            if artifact_type in merged["metadata"]["processing_stats"]:
                # Accumulate counts for same artifact type across batches
                existing = merged["metadata"]["processing_stats"][artifact_type]
                existing["count"] = existing.get("count", 0) + len(result.results)
                existing["errors"] = existing.get("errors", 0) + len(result.errors)
                existing["processed"] = existing.get("processed", 0) + result.metadata.get("processed", len(result.results))
            else:
                merged["metadata"]["processing_stats"][artifact_type] = {
                    "count": len(result.results),
                    "errors": len(result.errors),
                    **result.metadata
                }

            merged["metadata"]["total_results"] += len(result.results)
        
        if metrics:
            metrics.end_stage("aggregate_translations", success=True, items_processed=len(results))
        
        return merged
    except Exception as e:
        if metrics:
            metrics.end_stage("aggregate_translations", success=False)
        raise
