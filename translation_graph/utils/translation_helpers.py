import json
from typing import Dict, Any, Callable, List
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node


def build_translation_context(batch: ArtifactBatch) -> Dict[str, str]:
    """Build context dictionary for translation prompts."""
    return {
        "source_db": batch.context.get("source_db", "snowflake"),
        "target_db": batch.context.get("target_db", "databricks")
    }


def parse_artifact_json(artifact_json: str) -> Dict[str, Any]:
    """Parse JSON string representing an artifact."""
    return json.loads(artifact_json)


def invoke_llm_translation(llm, prompt: str) -> str:
    """Invoke LLM and extract content from response."""
    response = llm.invoke(prompt)
    return response.content if hasattr(response, 'content') else str(response)


def process_artifact_translation(
    batch: ArtifactBatch,
    artifact_type: str,
    translator_node: str,
    prompt_creator: Callable,
    artifact_name_key: str = "name"
) -> TranslationResult:
    """
    Generic function to process artifact translation.

    Args:
        batch: The artifact batch to translate
        artifact_type: Type of artifact (e.g., "tables", "views")
        translator_node: LLM node name for translation
        prompt_creator: Function that creates prompts (e.g., TablesPrompts.create_prompt)
        artifact_name_key: Key to extract artifact name from metadata for error messages

    Returns:
        TranslationResult with translated DDL
    """
    llm = create_llm_for_node(translator_node)
    results = []
    errors = []
    context = build_translation_context(batch)

    for artifact_json in batch.items:
        try:
            artifact_metadata = parse_artifact_json(artifact_json)
            prompt = prompt_creator(
                context=context,
                **{f"{artifact_type[:-1]}_metadata": json.dumps(artifact_metadata, indent=2)}
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                artifact_name = artifact_metadata.get(artifact_name_key, 'unknown')
                error_msg = f"LLM error for {artifact_type[:-1]} {artifact_name}: {str(e)}"
                results.append(f"-- Error generating DDL for {artifact_type[:-1]} {artifact_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing {artifact_type[:-1]}: {str(e)}")

    return TranslationResult(
        artifact_type=artifact_type,
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )

