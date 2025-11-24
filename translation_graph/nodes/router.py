from config.ddl_config import get_config
from prompts.router_prompts import RouterPrompts
from utils.types import ArtifactBatch


def artifact_router(batch: ArtifactBatch) -> str:
    """
    Route artifacts to the appropriate translation node.

    Args:
        batch: The artifact batch to route

    Returns:
        String indicating the target node: "tables", "views", "schemas", "procedures", or "roles"
    """
    config = get_config()
    llm = config.get_llm_for_node("smart_router")
    prompt = RouterPrompts.create_prompt()

    return "tables"
