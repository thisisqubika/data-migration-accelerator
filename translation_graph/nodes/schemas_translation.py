import time
import json
from prompts.schemas_prompts import SchemasPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.observability import get_observability


@handle_node_error("translate_schemas")
@retry_on_error(max_retries=3, retry_delay=1.0, logger_name="translate_schemas")
def translate_schemas(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate schema artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated schema DDL
    """
    obs = get_observability()
    logger = obs.get_logger("translate_schemas") if obs else None
    metrics = obs.get_metrics() if obs else None
    
    context = {
        "artifact_type": batch.artifact_type,
        "batch_size": len(batch.items)
    }
    
    if metrics:
        metrics.start_stage("translate_schemas", context)
    
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("schemas_translator")
        llm = create_llm_for_node("schemas_translator")

        # Process each schema in the batch
        results = []
        errors = []

        for schema_json in batch.items:
            try:
                # Parse the schema JSON
                schema_metadata = json.loads(schema_json)

                # Create prompt with context and schema metadata
                context_dict = {
                    "source_db": batch.context.get("source_db", "snowflake"),
                    "target_db": batch.context.get("target_db", "databricks")
                }

                prompt = SchemasPrompts.create_prompt(
                    context=context_dict,
                    schema_metadata=json.dumps(schema_metadata, indent=2)
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
                        logger.error(f"LLM error for schema {schema_metadata.get('schema_name', 'unknown')}", 
                                   context=context, error=str(e))
                    results.append(f"-- Error generating DDL for schema {schema_metadata.get('schema_name', 'unknown')}: {str(e)}")
                    errors.append(f"LLM error for schema {schema_metadata.get('schema_name', 'unknown')}: {str(e)}")

            except Exception as e:
                if logger:
                    logger.error(f"Error processing schema", context=context, error=str(e))
                errors.append(f"Error processing schema: {str(e)}")

        result = TranslationResult(
            artifact_type="schemas",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        if metrics:
            metrics.end_stage("translate_schemas", success=len(errors) == 0, items_processed=len(batch.items))
            metrics.record_artifact("schemas", count=len(batch.items))
        
        return result
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_schemas", success=False)
        raise
