from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.stages_prompts import StagesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_stages(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake stage artifacts to Databricks volumes.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated stage DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("stages_translator")

    results = []
    errors = []

    for stage_json in batch.items:
        try:
            stage_metadata = json.loads(stage_json)

            prompt_context = dict(batch.context or {})

            prompt = StagesPrompts.create_prompt(
                context=prompt_context,
                stage_metadata=json.dumps(stage_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating stage translation: {str(e)}")
                errors.append(f"LLM error for stage: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing stage: {str(e)}")

    return TranslationResult(
        artifact_type="stages",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
