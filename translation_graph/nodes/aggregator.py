from typing import List, Dict, Any, Optional
from utils.types import TranslationResult
from utils.error_handler import handle_node_error
from utils.observability import get_observability


@handle_node_error("aggregate_translations")
def aggregate_translations(*results: TranslationResult, failed_batches: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Aggregate multiple translation results into a final merged structure.

    Args:
        *results: Variable number of TranslationResult objects
        failed_batches: Optional list of failed batch dictionaries

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
            "masking_policies", "udfs", "procedures", "sequences", "file_formats"
        }
        
        merged = {artifact_type: [] for artifact_type in all_artifact_types}
        merged["metadata"] = {
            "total_results": 0,
            "errors": [],
            "processing_stats": {},
            "failed_batches_count": len(failed_batches) if failed_batches else 0
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

            # Update processing stats
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
