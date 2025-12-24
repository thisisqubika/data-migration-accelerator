from artifact_translation_package.prompts.views_prompts import ViewsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_views(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate view artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated view DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="views",
        translator_node="views_translator",
        prompt_creator=ViewsPrompts.create_prompt
    )
