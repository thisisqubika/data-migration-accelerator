from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.external_locations_prompts import ExternalLocationsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_external_locations(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake external location artifacts to Databricks external locations.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated external location DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("external_locations_translator")

    results = []
    errors = []

    for loc_json in batch.items:
        try:
            loc_metadata = json.loads(loc_json)

            prompt_context = dict(batch.context or {})

            prompt = ExternalLocationsPrompts.create_prompt(
                context=prompt_context,
                external_location_metadata=json.dumps(loc_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                text = response.content if hasattr(response, "content") else str(response)
                results.append(text.strip())
            except Exception as e:
                results.append(f"-- Error generating external location translation: {str(e)}")
                errors.append(f"LLM error for external location: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing external location: {str(e)}")

    return TranslationResult(
        artifact_type="external_locations",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )



