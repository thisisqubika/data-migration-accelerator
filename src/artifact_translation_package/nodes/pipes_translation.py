from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.pipes_prompts import PipesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_pipes(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake pipe (Snowpipe) artifacts to Databricks Auto Loader.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated pipe DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("pipes_translator")

    results = []
    errors = []

    for pipe_json in batch.items:
        try:
            pipe_metadata = json.loads(pipe_json)

            prompt_context = dict(batch.context or {})

            prompt = PipesPrompts.create_prompt(
                context=prompt_context,
                pipe_metadata=json.dumps(pipe_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating pipe translation: {str(e)}")
                errors.append(f"LLM error for pipe: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing pipe: {str(e)}")

    return TranslationResult(
        artifact_type="pipes",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
