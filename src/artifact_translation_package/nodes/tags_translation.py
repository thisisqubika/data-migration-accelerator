from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.tags_prompts import TagsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_tags(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake tag artifacts to Databricks Unity Catalog tags.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated tag DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("tags_translator")

    results = []
    errors = []

    for tag_json in batch.items:
        try:
            tag_metadata = json.loads(tag_json)

            prompt_context = dict(batch.context or {})

            prompt = TagsPrompts.create_prompt(
                context=prompt_context,
                tag_metadata=json.dumps(tag_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating tag translation: {str(e)}")
                errors.append(f"LLM error for tag: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing tag: {str(e)}")

    return TranslationResult(
        artifact_type="tags",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
