from artifact_translation_package.prompts.schemas_prompts import SchemasPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_schemas(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate schema artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated schema DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="schemas",
        translator_node="schemas_translator",
        prompt_creator=SchemasPrompts.create_prompt
    )
