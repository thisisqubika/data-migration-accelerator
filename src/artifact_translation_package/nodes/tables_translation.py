from artifact_translation_package.prompts.tables_prompts import TablesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_tables(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate table artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated table DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="tables",
        translator_node="tables_translator",
        prompt_creator=TablesPrompts.create_prompt
    )
