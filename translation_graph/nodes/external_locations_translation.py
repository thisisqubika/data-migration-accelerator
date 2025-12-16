from config.ddl_config import get_config
from prompts.external_locations_prompts import ExternalLocationsPrompts
from utils.types import ArtifactBatch, TranslationResult


def translate_external_locations(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake external location artifacts to Databricks external locations.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated external location DDL
    """
    config = get_config()
    llm = config.get_llm_for_node("external_locations_translator")
    prompt = ExternalLocationsPrompts.create_prompt()

    return TranslationResult(
        artifact_type="external_locations",
        results=["<placeholder translation result>"],
        errors=[],
        metadata={"count": len(batch.items)}
    )






