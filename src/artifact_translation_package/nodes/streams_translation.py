from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.streams_prompts import StreamsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_streams(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake stream artifacts to Databricks Delta Change Data Feed.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated stream DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("streams_translator")

    results = []
    errors = []

    for stream_json in batch.items:
        try:
            stream_metadata = json.loads(stream_json)

            prompt_context = dict(batch.context or {})

            prompt = StreamsPrompts.create_prompt(
                context=prompt_context,
                stream_metadata=json.dumps(stream_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating stream translation: {str(e)}")
                errors.append(f"LLM error for stream: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing stream: {str(e)}")

    return TranslationResult(
        artifact_type="streams",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
