from artifact_translation_package.prompts.views_prompts import ViewsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node
from artifact_translation_package.utils.translation_helpers import (
    build_translation_context,
    parse_artifact_json,
    invoke_llm_translation,
)
import json


def translate_views(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate view artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated view DDL
    """
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
                results.append(f"-- Error generating DDL for view {view_name}: {str(e)}")
                errors.append(f"LLM error for view {view_name}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing view: {str(e)}")

    return TranslationResult(
        artifact_type="views",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
