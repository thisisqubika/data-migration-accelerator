from artifact_translation_package.prompts.tags_prompts import TagsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_tags(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate tag artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated tag DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="tags",
        translator_node="tags_translator",
        prompt_creator=TagsPrompts.create_prompt
    )
