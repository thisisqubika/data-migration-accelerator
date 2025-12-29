import json
from prompts.views_prompts import ViewsPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.error_handler import handle_node_error, retry_on_error
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


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
    try:
        from config.ddl_config import get_config
        config = get_config()
        llm_config = config.get_llm_for_node("views_translator")
        llm = create_llm_for_node("views_translator")
        results = []
        errors = []
        context = build_translation_context(batch)

        for view_json in batch.items:
            try:
                view_metadata = parse_artifact_json(view_json)
                prompt = ViewsPrompts.create_prompt(
                    context=context,
                    view_metadata=json.dumps(view_metadata, indent=2)
                )

                try:
                    ddl_result = invoke_llm_translation(llm, prompt)
                    results.append(ddl_result.strip())
                except Exception as e:
                    view_name = view_metadata.get('view_name', 'unknown')
                    error_msg = f"LLM error for view {view_name}: {str(e)}"
                    if logger:
                        logger.error(error_msg, context=context_dict, error=str(e))
                    results.append(f"-- Error generating DDL for view {view_name}: {str(e)}")
                    errors.append(error_msg)

            except Exception as e:
                errors.append(f"Error processing view: {str(e)}")

        result = TranslationResult(
            artifact_type="views",
            results=results,
            errors=errors,
            metadata={"count": len(batch.items), "processed": len(results)}
        )
        
        return result
