from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.grants_prompts import GrantsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_grants(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake grant artifacts to Databricks Unity Catalog privileges.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated grant DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("grants_translator")

    results = []
    errors = []

    for grant_json in batch.items:
        try:
            grant_metadata = json.loads(grant_json)

            # Build prompt context with sensible defaults, merged with any
            # batch-provided context so prompts always receive `source_db`
            # and `target_db` keys like other nodes.
            prompt_context = dict(batch.context or {})
            prompt_context.setdefault("source_db", "snowflake")
            prompt_context.setdefault("target_db", "databricks")

            prompt = GrantsPrompts.create_prompt(
                context=prompt_context,
                grant_metadata=json.dumps(grant_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating grant translation: {str(e)}")
                errors.append(f"LLM error for grant: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing grant: {str(e)}")

    return TranslationResult(
        artifact_type="grants",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
