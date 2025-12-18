import json
from artifact_translation_package.prompts.procedures_prompts import ProceduresPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node


def translate_procedures(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate procedure artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated procedure DDL
    """
    llm = create_llm_for_node("procedures_translator")

    # Process each procedure in the batch
    results = []
    errors = []

    for procedure_json in batch.items:
        try:
            # Parse the procedure JSON
            procedure_metadata = json.loads(procedure_json)

            # Create prompt with context and procedure metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = ProceduresPrompts.create_prompt(
                context=context,
                procedure_metadata=json.dumps(procedure_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for procedure {procedure_metadata.get('procedure_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for procedure {procedure_metadata.get('procedure_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing procedure: {str(e)}")

    return TranslationResult(
        artifact_type="procedures",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
