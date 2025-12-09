from typing import List, Dict, Any
from artifact_translation_package.utils.types import TranslationResult


def aggregate_translations(*results: TranslationResult) -> Dict[str, Any]:
    """
    Aggregate multiple translation results into a final merged structure.

    Args:
        *results: Variable number of TranslationResult objects

    Returns:
        Dictionary with merged results:
        {
            "tables": [...],
            "views": [...],
            "schemas": [...],
            "procedures": [...],
            "roles": [...],
            "metadata": {...}
        }
    """
    all_artifact_types = {
        "databases", "schemas", "tables", "views", "stages", "external_locations",
        "streams", "pipes", "roles", "grants", "tags", "comments",
        "masking_policies", "udfs", "procedures", "sequences", "file_formats"
    }
    
    merged = {artifact_type: [] for artifact_type in all_artifact_types}
    merged["metadata"] = {
        "total_results": 0,
        "errors": [],
        "processing_stats": {}
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

    return merged
