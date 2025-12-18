from artifact_translation_package.prompts.schemas_prompts import SchemasPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node
import json


def translate_schemas(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate schema artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated schema DDL
    """
    llm = create_llm_for_node("schemas_translator")

    # Process each schema in the batch
    results = []
    errors = []

    for schema_json in batch.items:
        try:
            schema_metadata = json.loads(schema_json)

            # Create prompt with context and schema metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = SchemasPrompts.create_prompt(
                context=context,
                schema_metadata=json.dumps(schema_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for schema {schema_metadata.get('schema_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for schema {schema_metadata.get('schema_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing schema: {str(e)}")

    return TranslationResult(
        artifact_type="schemas",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
