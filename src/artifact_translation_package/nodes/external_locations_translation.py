from artifact_translation_package.prompts.external_locations_prompts import ExternalLocationsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_external_locations(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate external location artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated external location DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="external_locations",
        translator_node="external_locations_translator",
        prompt_creator=ExternalLocationsPrompts.create_prompt
    )
