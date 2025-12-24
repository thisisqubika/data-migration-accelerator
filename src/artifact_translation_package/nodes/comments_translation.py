from artifact_translation_package.prompts.comments_prompts import CommentsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_comments(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate comment artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated comment DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="comments",
        translator_node="comments_translator",
        prompt_creator=CommentsPrompts.create_prompt
    )
