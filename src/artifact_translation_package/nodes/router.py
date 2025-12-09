from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.prompts.router_prompts import RouterPrompts
from artifact_translation_package.utils.types import ArtifactBatch


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
        "comments", "masking_policies", "udfs", "procedures", "sequences", "file_formats"
    """
    if batch.artifact_type:
        valid_nodes = {
            "databases", "schemas", "tables", "views", "stages", "external_locations",
            "streams", "pipes", "roles", "grants", "tags", "comments",
            "masking_policies", "udfs", "procedures", "sequences", "file_formats"
        }
        if batch.artifact_type in valid_nodes:
            return batch.artifact_type
    
    config = get_config()
    llm = config.get_llm_for_node("smart_router")
    prompt = RouterPrompts.create_prompt()

    ddl_content = "\n".join(batch.items) if batch.items else ""
    routing_prompt = f"{prompt}\n\nDDL Content:\n{ddl_content}"

    response = llm.invoke(routing_prompt)

    response_text = response.content if hasattr(response, 'content') else str(response)

    valid_nodes = {
        "databases", "schemas", "tables", "views", "stages", "external_locations",
        "streams", "pipes", "roles", "grants", "tags", "comments",
        "masking_policies", "udfs", "procedures", "sequences", "file_formats"
    }

    for node in valid_nodes:
        if node.lower() in response_text.lower():
            return node

    return "tables"
