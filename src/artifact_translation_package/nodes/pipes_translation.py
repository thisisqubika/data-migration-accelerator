from artifact_translation_package.prompts.pipes_prompts import PipesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_pipes(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate pipe artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated pipe DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="pipes",
        translator_node="pipes_translator",
        prompt_creator=PipesPrompts.create_prompt
    )
