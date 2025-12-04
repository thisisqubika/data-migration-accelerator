import json
from prompts.database_prompts import DatabasePrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


def translate_databases(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate database artifacts to Databricks catalogs.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated database DDL
    """
    llm = create_llm_for_node("database_translator")
    results = []
    errors = []
    context = build_translation_context(batch)

    for database_json in batch.items:
        try:
            database_metadata = parse_artifact_json(database_json)
            prompt = DatabasePrompts.create_prompt(
                context=context,
                ddl=json.dumps(database_metadata, indent=2)
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                database_name = database_metadata.get('database_name', 'unknown')
                error_msg = f"LLM error for database {database_name}: {str(e)}"
                results.append(f"-- Error generating DDL for database {database_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing database: {str(e)}")

    return TranslationResult(
        artifact_type="databases",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
