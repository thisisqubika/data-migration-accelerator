import json
from prompts.sequences_prompts import SequencesPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


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
    results = []
    errors = []
    context = build_translation_context(batch)

    for sequence_json in batch.items:
        try:
            sequence_metadata = parse_artifact_json(sequence_json)
            prompt = SequencesPrompts.create_prompt(
                context=context,
                sequence_metadata=json.dumps(sequence_metadata, indent=2)
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                sequence_name = sequence_metadata.get('sequence_name', 'unknown')
                error_msg = f"LLM error for sequence {sequence_name}: {str(e)}"
                results.append(f"-- Error generating DDL for sequence {sequence_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing sequence: {str(e)}")

    return TranslationResult(
        artifact_type="sequences",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )

