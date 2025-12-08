import time
import json
from prompts.procedures_prompts import ProceduresPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.observability import get_observability


@handle_node_error("translate_procedures")
@retry_on_error(max_retries=3, retry_delay=1.0, logger_name="translate_procedures")
def translate_procedures(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate procedure artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated procedure DDL
    """
    obs = get_observability()
    logger = obs.get_logger("translate_procedures") if obs else None
    metrics = obs.get_metrics() if obs else None
    
    context = {
        "artifact_type": batch.artifact_type,
        "batch_size": len(batch.items)
    }
    
    if metrics:
        metrics.start_stage("translate_procedures", context)
    
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("procedures_translator")
        llm = create_llm_for_node("procedures_translator")

        # Process each procedure in the batch
        results = []
        errors = []

        for procedure_json in batch.items:
            try:
                # Parse the procedure JSON
                procedure_metadata = json.loads(procedure_json)

                # Create prompt with context and procedure metadata
                context_dict = {
                    "source_db": batch.context.get("source_db", "snowflake"),
                    "target_db": batch.context.get("target_db", "databricks")
                }

                prompt = ProceduresPrompts.create_prompt(
                    context=context_dict,
                    procedure_metadata=json.dumps(procedure_metadata, indent=2)
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
                        logger.error(f"LLM error for procedure {procedure_metadata.get('procedure_name', 'unknown')}", 
                                   context=context, error=str(e))
                    results.append(f"-- Error generating DDL for procedure {procedure_metadata.get('procedure_name', 'unknown')}: {str(e)}")
                    errors.append(f"LLM error for procedure {procedure_metadata.get('procedure_name', 'unknown')}: {str(e)}")

            except Exception as e:
                if logger:
                    logger.error(f"Error processing procedure", context=context, error=str(e))
                errors.append(f"Error processing procedure: {str(e)}")

        result = TranslationResult(
            artifact_type="procedures",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        if metrics:
            metrics.end_stage("translate_procedures", success=len(errors) == 0, items_processed=len(batch.items))
            metrics.record_artifact("procedures", count=len(batch.items))
        
        return result
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_procedures", success=False)
        raise
