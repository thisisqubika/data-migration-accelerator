import json
from prompts.procedures_prompts import ProceduresPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.observability import get_observability
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


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
    
    context_dict = {
        "artifact_type": batch.artifact_type,
        "batch_size": len(batch.items)
    }
    
    if metrics:
        metrics.start_stage("translate_procedures", context_dict)
    
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("procedures_translator")
        llm = create_llm_for_node("procedures_translator")
        results = []
        errors = []
        context = build_translation_context(batch)

        for procedure_json in batch.items:
            try:
                procedure_metadata = parse_artifact_json(procedure_json)
                prompt = ProceduresPrompts.create_prompt(
                    context=context,
                    procedure_metadata=json.dumps(procedure_metadata, indent=2)
                )

                try:
                    ddl_result = invoke_llm_translation(llm, prompt)
                    results.append(ddl_result.strip())
                except Exception as e:
                    procedure_name = procedure_metadata.get('procedure_name', 'unknown')
                    error_msg = f"LLM error for procedure {procedure_name}: {str(e)}"
                    if logger:
                        logger.error(error_msg, context=context_dict, error=str(e))
                    results.append(f"-- Error generating DDL for procedure {procedure_name}: {str(e)}")
                    errors.append(error_msg)

            except Exception as e:
                if logger:
                    logger.error(f"Error processing procedure", context=context_dict, error=str(e))
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
