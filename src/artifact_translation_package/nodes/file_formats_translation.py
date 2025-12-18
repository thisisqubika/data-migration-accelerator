from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.file_formats_prompts import FileFormatsPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
import json


def translate_file_formats(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake file format artifacts to Databricks file formats.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated file format DDL
    """
    config = get_config()
    from artifact_translation_package.utils.llm_utils import create_llm_for_node
    llm = create_llm_for_node("file_formats_translator")

    results = []
    errors = []

    for ff_json in batch.items:
        try:
            file_format_metadata = json.loads(ff_json)

            # Build a minimal context for the prompt; preserve any existing
            # context entries provided by the batch.
            prompt_context = dict(batch.context or {})

            prompt = FileFormatsPrompts.create_prompt(
                context=prompt_context,
                file_format_metadata=json.dumps(file_format_metadata, indent=2),
            )

            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, "content") else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for file format {file_format_metadata.get('name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for file format {file_format_metadata.get('name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing file format: {str(e)}")

    return TranslationResult(
        artifact_type="file_formats",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)},
    )



