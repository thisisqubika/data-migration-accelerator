from artifact_translation_package.prompts.roles_prompts import RolesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_roles(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate role artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated role DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="roles",
        translator_node="roles_translator",
        prompt_creator=RolesPrompts.create_prompt
    )
