from artifact_translation_package.prompts.udfs_prompts import UDFsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node


def translate_udfs(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake SQL UDF artifacts to Databricks Unity Catalog SQL UDFs.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated UDF DDL
    """
    llm = create_llm_for_node("udfs_translator")

    # Process each UDF in the batch
    results = []
    errors = []

    for udf_json in batch.items:
        try:
            # Parse the UDF JSON
            import json
            udf_metadata = json.loads(udf_json)

            # Create prompt with context and UDF metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = UDFsPrompts.create_prompt(
                context=context,
                function_metadata=json.dumps(udf_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for UDF {udf_metadata.get('function_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for UDF {udf_metadata.get('function_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing UDF: {str(e)}")

    return TranslationResult(
        artifact_type="udfs",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
