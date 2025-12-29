from artifact_translation_package.prompts.streams_prompts import StreamsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_streams(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate stream artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated stream DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="streams",
        translator_node="streams_translator",
        prompt_creator=StreamsPrompts.create_prompt
    )
