from artifact_translation_package.prompts.procedures_prompts import ProceduresPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_procedures(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate procedure artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated procedure DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="procedures",
        translator_node="procedures_translator",
        prompt_creator=ProceduresPrompts.create_prompt
    )
