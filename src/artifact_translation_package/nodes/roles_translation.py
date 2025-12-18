from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.roles_prompts import RolesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_roles(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate role artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated role DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("roles_translator")

    results = []
    errors = []

    for role_json in batch.items:
        try:
            role_metadata = json.loads(role_json)

            prompt_context = dict(batch.context or {})

            prompt = RolesPrompts.create_prompt(
                context=prompt_context,
                role_metadata=json.dumps(role_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating role translation: {str(e)}")
                errors.append(f"LLM error for role: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing role: {str(e)}")

    return TranslationResult(
        artifact_type="roles",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
