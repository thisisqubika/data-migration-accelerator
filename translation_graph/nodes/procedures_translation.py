import json
from prompts.procedures_prompts import ProceduresPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
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
                errors.append(f"Error processing procedure: {str(e)}")

        result = TranslationResult(
            artifact_type="procedures",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        return result
