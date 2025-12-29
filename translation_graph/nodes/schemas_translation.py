import json
from prompts.schemas_prompts import SchemasPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


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
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("schemas_translator")
        llm = create_llm_for_node("schemas_translator")
        results = []
        errors = []
        context = build_translation_context(batch)

        for schema_json in batch.items:
            try:
                schema_metadata = parse_artifact_json(schema_json)
                prompt = SchemasPrompts.create_prompt(
                    context=context,
                    schema_metadata=json.dumps(schema_metadata, indent=2)
                )

                try:
                    ddl_result = invoke_llm_translation(llm, prompt)
                    results.append(ddl_result.strip())
                except Exception as e:
                    schema_name = schema_metadata.get('schema_name', 'unknown')
                    error_msg = f"LLM error for schema {schema_name}: {str(e)}"
                    if logger:
                        logger.error(error_msg, context=context_dict, error=str(e))
                    results.append(f"-- Error generating DDL for schema {schema_name}: {str(e)}")
                    errors.append(error_msg)

            except Exception as e:
                errors.append(f"Error processing schema: {str(e)}")

        result = TranslationResult(
            artifact_type="schemas",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        return result
