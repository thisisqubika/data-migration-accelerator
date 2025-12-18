from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.comments_prompts import CommentsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_comments(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake comment artifacts to Databricks comments.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated comment DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("comments_translator")

    results = []
    errors = []

    for comment_json in batch.items:
        try:
            comment_metadata = json.loads(comment_json)

            prompt_context = dict(batch.context or {})

            prompt = CommentsPrompts.create_prompt(
                context=prompt_context,
                comment_metadata=json.dumps(comment_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating comment translation: {str(e)}")
                errors.append(f"LLM error for comment: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing comment: {str(e)}")

    return TranslationResult(
        artifact_type="comments",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
