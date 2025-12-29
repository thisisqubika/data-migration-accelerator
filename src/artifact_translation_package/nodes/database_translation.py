from artifact_translation_package.prompts.database_prompts import DatabasePrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_databases(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate database artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated database DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="databases",
        translator_node="databases_translator",
        prompt_creator=DatabasePrompts.create_prompt
    )
