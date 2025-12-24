from artifact_translation_package.prompts.stages_prompts import StagesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_stages(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate stage artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated stage DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="stages",
        translator_node="stages_translator",
        prompt_creator=StagesPrompts.create_prompt
    )
