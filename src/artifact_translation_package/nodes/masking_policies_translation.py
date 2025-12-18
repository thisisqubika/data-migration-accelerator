from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.masking_policies_prompts import MaskingPoliciesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_masking_policies(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake masking policy artifacts to Databricks Unity Catalog column masks (UDFs).

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated masking policy DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("masking_policies_translator")

    results = []
    errors = []

    for mp_json in batch.items:
        try:
            mp_metadata = json.loads(mp_json)

            prompt_context = dict(batch.context or {})

            prompt = MaskingPoliciesPrompts.create_prompt(
                context=prompt_context,
                masking_policy_metadata=json.dumps(mp_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating masking policy translation: {str(e)}")
                errors.append(f"LLM error for masking policy: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing masking policy: {str(e)}")

    return TranslationResult(
        artifact_type="masking_policies",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )
