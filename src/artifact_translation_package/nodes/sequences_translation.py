import json
from artifact_translation_package.prompts.sequences_prompts import SequencesPrompts
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node


def translate_sequences(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake sequence artifacts to Databricks equivalents.

    Note: Databricks doesn't have sequences like Snowflake. This translator
    provides guidance on alternative approaches.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with sequence migration guidance
    """
    llm = create_llm_for_node("sequences_translator")

    # Process each sequence in the batch
    results = []
    errors = []

    for sequence_json in batch.items:
        try:
            # Parse the sequence JSON
            sequence_metadata = json.loads(sequence_json)

            # Create prompt with context and sequence metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = SequencesPrompts.create_prompt(
                context=context,
                sequence_metadata=json.dumps(sequence_metadata, indent=2)
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for sequence {sequence_metadata.get('sequence_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for sequence {sequence_metadata.get('sequence_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing sequence: {str(e)}")

    return TranslationResult(
        artifact_type="sequences",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )

