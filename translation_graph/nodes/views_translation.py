import time
import json
from prompts.views_prompts import ViewsPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.observability import get_observability


@handle_node_error("translate_views")
@retry_on_error(max_retries=3, retry_delay=1.0, logger_name="translate_views")
def translate_views(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate view artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated view DDL
    """
    obs = get_observability()
    logger = obs.get_logger("translate_views") if obs else None
    metrics = obs.get_metrics() if obs else None
    
    context = {
        "artifact_type": batch.artifact_type,
        "batch_size": len(batch.items)
    }
    
    if metrics:
        metrics.start_stage("translate_views", context)
    
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("views_translator")
        llm = create_llm_for_node("views_translator")

        # Process each view in the batch
        results = []
        errors = []

        for view_json in batch.items:
            try:
                # Parse the view JSON
                view_metadata = json.loads(view_json)

                # Create prompt with context and view metadata
                context_dict = {
                    "source_db": batch.context.get("source_db", "snowflake"),
                    "target_db": batch.context.get("target_db", "databricks")
                }

                prompt = ViewsPrompts.create_prompt(
                    context=context_dict,
                    ddl=view_metadata.get('view_definition', '')
                )

                # Call the LLM to generate DDL
                ai_start = time.time()
                try:
                    response = llm.invoke(prompt)
                    ai_latency = time.time() - ai_start
                    
                    if metrics:
                        metrics.record_ai_call(
                            provider=llm_config.provider,
                            model=llm_config.model,
                            latency=ai_latency,
                            error=False
                        )
                    
                    ddl_result = response.content if hasattr(response, 'content') else str(response)
                    results.append(ddl_result.strip())
                except Exception as e:
                    ai_latency = time.time() - ai_start
                    if metrics:
                        metrics.record_ai_call(
                            provider=llm_config.provider,
                            model=llm_config.model,
                            latency=ai_latency,
                            error=True
                        )
                    if logger:
                        logger.error(f"LLM error for view {view_metadata.get('view_name', 'unknown')}", 
                                   context=context, error=str(e))
                    results.append(f"-- Error generating DDL for view {view_metadata.get('view_name', 'unknown')}: {str(e)}")
                    errors.append(f"LLM error for view {view_metadata.get('view_name', 'unknown')}: {str(e)}")

            except Exception as e:
                if logger:
                    logger.error(f"Error processing view", context=context, error=str(e))
                errors.append(f"Error processing view: {str(e)}")

        result = TranslationResult(
            artifact_type="views",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        if metrics:
            metrics.end_stage("translate_views", success=len(errors) == 0, items_processed=len(batch.items))
            metrics.record_artifact("views", count=len(batch.items))
        
        return result
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_views", success=False)
        raise
