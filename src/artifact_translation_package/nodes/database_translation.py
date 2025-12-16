from artifact_translation_package.prompts.database_prompts import DatabasePrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node


def translate_databases(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate database artifacts to Databricks catalogs.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated database DDL
    """
    llm = create_llm_for_node("database_translator")

    # Process each database in the batch
    results = []
    errors = []

    for database_json in batch.items:
        try:
            # Parse the database JSON
            import json
            database_metadata = json.loads(database_json)

            # Create prompt with context and database metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = DatabasePrompts.create_prompt(
                context=context,
                ddl=json.dumps(database_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for database {database_metadata.get('database_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for database {database_metadata.get('database_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing database: {str(e)}")

    return TranslationResult(
        artifact_type="databases",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
