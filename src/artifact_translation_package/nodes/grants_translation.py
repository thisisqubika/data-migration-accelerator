from artifact_translation_package.prompts.grants_prompts import GrantsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_grants(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate grant artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated grant DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="grants",
        translator_node="grants_translator",
        prompt_creator=GrantsPrompts.create_prompt
    )
