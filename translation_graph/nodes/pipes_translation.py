from prompts.pipes_prompts import PipesPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node
from utils.translation_helpers import build_translation_context, parse_artifact_json, invoke_llm_translation


def translate_pipes(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate Snowflake pipe (Snowpipe) artifacts to Databricks Auto Loader.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated pipe DDL
    """
    llm = create_llm_for_node("pipes_translator")
    results = []
    errors = []
    context = build_translation_context(batch)

    for pipe_json in batch.items:
        try:
            pipe_metadata = parse_artifact_json(pipe_json)
            prompt = PipesPrompts.create_prompt(
                context=context,
                ddl=pipe_json
            )

            try:
                ddl_result = invoke_llm_translation(llm, prompt)
                results.append(ddl_result.strip())
            except Exception as e:
                pipe_name = pipe_metadata.get('pipe_name', 'unknown')
                error_msg = f"LLM error for pipe {pipe_name}: {str(e)}"
                results.append(f"-- Error generating DDL for pipe {pipe_name}: {str(e)}")
                errors.append(error_msg)

        except Exception as e:
            errors.append(f"Error processing pipe: {str(e)}")

    return TranslationResult(
        artifact_type="pipes",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
