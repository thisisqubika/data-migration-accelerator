from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.router_prompts import RouterPrompts
from artifact_translation_package.utils.types import ArtifactBatch
from artifact_translation_package.utils.llm_utils import create_llm_for_node


def artifact_router(batch: ArtifactBatch) -> str:
    """
    Route artifacts to the appropriate translation node.
    
    If artifact_type is already set in the batch, returns it directly without LLM routing.
    Otherwise, uses LLM to determine the artifact type from DDL content.

    Args:
        batch: The artifact batch to route

    Returns:
        String indicating the target node: "databases", "schemas", "tables", "views",
        "stages", "external_locations", "streams", "pipes", "roles", "grants", "tags",
        "comments", "masking_policies", "udfs", "procedures", "sequences"
    """
    if batch.artifact_type:
        # Note: certain artifact types (previously file_formats) are intentionally excluded from runtime routing
        valid_nodes = {
            "databases", "schemas", "tables", "views", "stages", "external_locations",
            "streams", "pipes", "roles", "grants", "tags", "comments",
            "masking_policies", "udfs", "procedures", "sequences"
        }
        # If the batch already declares an artifact_type and it's supported,
        # return it directly. If it's declared but not supported by the runtime
        # graph, return the aggregator to skip translation for that batch.
        if batch.artifact_type in valid_nodes:
            return batch.artifact_type
        # Return the aggregator node to skip translation for unsupported
        # artifact types so the batch will be ignored by the translation
        # nodes and merged as metadata only.
        return "aggregator"
    
    config = get_config()
    llm = create_llm_for_node("smart_router")
    prompt_context = dict(batch.context or {})
    prompt = RouterPrompts.create_prompt(context=prompt_context)

    ddl_content = "\n".join(batch.items) if batch.items else ""
    routing_prompt = f"{prompt}\n\nDDL Content:\n{ddl_content}"

    response = llm.invoke(routing_prompt)

    response_text = response.content if hasattr(response, 'content') else str(response)

    valid_nodes = {
        "databases", "schemas", "tables", "views", "stages", "external_locations",
        "streams", "pipes", "roles", "grants", "tags", "comments",
        "masking_policies", "udfs", "procedures", "sequences"
    }

    for node in valid_nodes:
        if node.lower() in response_text.lower():
            return node

    return "tables"
