import json
from typing import Dict, Any, Callable, List, Optional
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node
from artifact_translation_package.utils.observability import get_observability


def build_translation_context(batch: ArtifactBatch) -> Dict[str, str]:
    """
    Build context dictionary for translation prompts.
    
    Args:
        batch: The artifact batch
        
    Returns:
        Dictionary with source_db and target_db context
    """
    return {
        "source_db": batch.context.get("source_db", "snowflake"),
        "target_db": batch.context.get("target_db", "databricks")
    }


def parse_artifact_json(artifact_json: str) -> Dict[str, Any]:
    """
    Parse JSON string representing an artifact.
    
    Args:
        artifact_json: JSON string to parse
        
    Returns:
        Parsed dictionary
        
    Raises:
        json.JSONDecodeError: If JSON is invalid
    """
    return json.loads(artifact_json)


def invoke_llm_translation(llm, prompt: str) -> str:
    """
    Invoke LLM and extract content from response.
    
    Args:
        llm: LLM instance to invoke
        prompt: Prompt string to send to LLM
        
    Returns:
        LLM response content as string
    """
    response = llm.invoke(prompt)
    return response.content if hasattr(response, 'content') else str(response)


def get_artifact_name(artifact_metadata: Dict[str, Any], artifact_type: str) -> str:
    """
    Extract artifact name from metadata with fallback.
    
    Args:
        artifact_metadata: Parsed artifact metadata
        artifact_type: Type of artifact (e.g., "tables", "views")
        
    Returns:
        Artifact name or 'unknown' if not found
    """
    # Try common name keys based on artifact type
    name_keys = [
        f"{artifact_type[:-1]}_name",  # e.g., "table_name", "view_name"
        "name",
        "object_name",
        "identifier"
    ]
    
    for key in name_keys:
        if key in artifact_metadata:
            return artifact_metadata[key]
    
    return "unknown"


def create_error_message(
    artifact_type: str,
    artifact_name: str,
    error: Exception,
    error_type: str = "LLM"
) -> str:
    """
    Create standardized error message for translation failures.
    
    Args:
        artifact_type: Type of artifact (e.g., "tables", "views")
        artifact_name: Name of the artifact
        error: Exception that occurred
        error_type: Type of error ("LLM" or "processing")
        
    Returns:
        Formatted error message
    """
    artifact_singular = artifact_type[:-1] if artifact_type.endswith('s') else artifact_type
    return f"{error_type} error for {artifact_singular} {artifact_name}: {str(error)}"


def process_artifact_translation(
    batch: ArtifactBatch,
    artifact_type: str,
    translator_node: str,
    prompt_creator: Callable,
    artifact_name_key: Optional[str] = None
) -> TranslationResult:
    """
    Generic function to process artifact translation.

    Args:
        batch: The artifact batch to translate
        artifact_type: Type of artifact (e.g., "tables", "views")
        translator_node: LLM node name for translation
        prompt_creator: Function that creates prompts (e.g., TablesPrompts.create_prompt)
        artifact_name_key: Deprecated parameter - use get_artifact_name() instead

    Returns:
        TranslationResult with translated DDL
    """
    llm = create_llm_for_node(translator_node)
    results = []
    errors = []
    context = build_translation_context(batch)
    
    # Get observability metrics
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    
    stage_name = f"translate_{artifact_type}"
    if metrics:
        metrics.start_stage(stage_name, {"batch_size": len(batch.items)})

    for artifact_json in batch.items:
        try:
            artifact_metadata = parse_artifact_json(artifact_json)
            artifact_name = get_artifact_name(artifact_metadata, artifact_type)
            
            # Build prompt with context and metadata
            prompt = prompt_creator(
                context=context,
                metadata=json.dumps(artifact_metadata, indent=2)
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
                
                # Record successful artifact to metrics
                if metrics:
                    metrics.record_artifact(artifact_type, count=1)
            except Exception as e:
                error_msg = create_error_message(artifact_type, artifact_name, e, "LLM")
                results.append(f"-- Error generating DDL for {artifact_type[:-1]} {artifact_name}: {str(e)}")
                errors.append(error_msg)
                
                # Record artifact to metrics (even if it had an error)
                if metrics:
                    metrics.record_artifact(artifact_type, count=1)

        except Exception as e:
            artifact_name = get_artifact_name(
                parse_artifact_json(artifact_json),
                artifact_type
            )
            error_msg = create_error_message(artifact_type, artifact_name, e, "processing")
            errors.append(error_msg)
            
            # Record artifact to metrics (even if it had an error)
            if metrics:
                metrics.record_artifact(artifact_type, count=1)

    if metrics:
        metrics.end_stage(stage_name, success=len(errors) == 0, items_processed=len(results))

    return TranslationResult(
        artifact_type=artifact_type,
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results), "errors": len(errors)}
    )
