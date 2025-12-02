from prompts.views_prompts import ViewsPrompts
from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import create_llm_for_node


def translate_views(batch: ArtifactBatch) -> TranslationResult:
    """
    Translate view artifacts.

    Args:
        batch: The artifact batch to translate

    Returns:
        TranslationResult with translated view DDL
    """
    llm = create_llm_for_node("views_translator")

    # Process each view in the batch
    results = []
    errors = []

    for view_json in batch.items:
        try:
            # Parse the view JSON
            import json
            view_metadata = json.loads(view_json)

            # Create prompt with context and view metadata
            context = {
                "source_db": batch.context.get("source_db", "snowflake"),
                "target_db": batch.context.get("target_db", "databricks")
            }

            prompt = ViewsPrompts.create_prompt(
                context=context,
                ddl=view_metadata.get('view_definition', '')
            )

            # Call the LLM to generate DDL
            try:
                response = llm.invoke(prompt)
                ddl_result = response.content if hasattr(response, 'content') else str(response)
                results.append(ddl_result.strip())
            except Exception as e:
                results.append(f"-- Error generating DDL for view {view_metadata.get('view_name', 'unknown')}: {str(e)}")
                errors.append(f"LLM error for view {view_metadata.get('view_name', 'unknown')}: {str(e)}")

        except Exception as e:
            errors.append(f"Error processing view: {str(e)}")

    return TranslationResult(
        artifact_type="views",
        results=results,
        errors=errors,
        metadata={"count": len(batch.items), "processed": len(results)}
    )
