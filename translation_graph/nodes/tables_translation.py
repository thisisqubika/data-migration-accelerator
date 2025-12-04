import json
from prompts.tables_prompts import TablesPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


def translate_tables(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate table artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated table DDL
    """
    llm = create_llm_for_node("tables_translator")
    results = []
    errors = []
    context = build_translation_context(batch)

    for table_json in batch.items:
        try:
            table_metadata = parse_artifact_json(table_json)
            prompt = TablesPrompts.create_prompt(
                context=context,
                table_metadata=json.dumps(table_metadata, indent=2)
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                table_name = table_metadata.get('table_name', 'unknown')
                error_msg = f"LLM error for table {table_name}: {str(e)}"
                results.append(f"-- Error generating DDL for table {table_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing table: {str(e)}")

    return TranslationResult(
        artifact_type="tables",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
