from artifact_translation_package.prompts.masking_policies_prompts import MaskingPoliciesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.translation_helpers import process_artifact_translation


def translate_masking_policies(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate masking policy artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated masking policy DDL
    """
    return process_artifact_translation(
        batch=batch,
        artifact_type="masking_policies",
        translator_node="masking_policies_translator",
        prompt_creator=MaskingPoliciesPrompts.create_prompt
    )
