import json
from prompts.udfs_prompts import UDFsPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


def translate_udfs(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake SQL UDF artifacts to Databricks Unity Catalog SQL UDFs.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated UDF DDL
    """
    llm = create_llm_for_node("udfs_translator")
    results = []
    errors = []
    context = build_translation_context(batch)

    for udf_json in batch.items:
        try:
            udf_metadata = parse_artifact_json(udf_json)
            prompt = UDFsPrompts.create_prompt(
                context=context,
                function_metadata=json.dumps(udf_metadata, indent=2)
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                function_name = udf_metadata.get('function_name', 'unknown')
                error_msg = f"LLM error for UDF {function_name}: {str(e)}"
                results.append(f"-- Error generating DDL for UDF {function_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing UDF: {str(e)}")

    return TranslationResult(
        artifact_type="udfs",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
