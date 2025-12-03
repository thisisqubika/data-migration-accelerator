from prompts.tables_prompts import TablesPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node


def translate_tables(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate table artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated table DDL
    """
    llm = create_llm_for_node("tables_translator")

    # Process each table in the batch
    results = []
    errors = []

    for table_json in batch.items:
        try:
            # Parse the table JSON
            import json
            table_metadata = json.loads(table_json)

            # Create prompt with context and table metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = TablesPrompts.create_prompt(
                context=context,
                table_metadata=json.dumps(table_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for table {table_metadata.get('table_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for table {table_metadata.get('table_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing table: {str(e)}")

    return TranslationResult(
        artifact_type="tables",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
