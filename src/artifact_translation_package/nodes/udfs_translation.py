from artifact_translation_package.prompts.udfs_prompts import UDFsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_udfs(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate UDF artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated UDF DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="udfs",
        translator_node="udfs_translator",
        prompt_creator=UDFsPrompts.create_prompt
    )
