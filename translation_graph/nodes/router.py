from config.ddl_config import get_config
from prompts.router_prompts import RouterPrompts
from utils.types import ArtifactBatch
from utils.error_handler import handle_node_error
from utils.observability import get_observability


@handle_node_error("artifact_router")
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
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    logger = obs.get_logger("artifact_router") if obs else None
    
    context = {
        "artifact_type": batch.artifact_type,
        "batch_size": len(batch.items)
    }
    
    if metrics:
        metrics.start_stage("artifact_router", context)
    
    try:
        if batch.artifact_type:
            valid_nodes = {
                "databases", "schemas", "tables", "views", "stages", "external_locations",
                "streams", "pipes", "roles", "grants", "tags", "comments",
                "masking_policies", "udfs", "procedures", "sequences", "file_formats"
            }
            if batch.artifact_type in valid_nodes:
                if metrics:
                    metrics.end_stage("artifact_router", success=True)
                return batch.artifact_type
        
        config = get_config()
        llm = config.get_llm_for_node("smart_router")
        prompt = RouterPrompts.create_prompt()

        ddl_content = "\n".join(batch.items) if batch.items else ""
        routing_prompt = f"{prompt}\n\nDDL Content:\n{ddl_content}"

        # Track AI call
        import time
        ai_start = time.time()
        try:
            response = llm.invoke(routing_prompt)
            ai_latency = time.time() - ai_start
            
            if metrics:
                llm_config = config.get_llm_for_node("smart_router")
                metrics.record_ai_call(
                    provider=llm_config.provider,
                    model=llm_config.model,
                    latency=ai_latency,
                    error=False
                )
        except Exception as e:
            ai_latency = time.time() - ai_start
            if metrics:
                llm_config = config.get_llm_for_node("smart_router")
                metrics.record_ai_call(
                    provider=llm_config.provider,
                    model=llm_config.model,
                    latency=ai_latency,
                    error=True
                )
            if logger:
                logger.error(f"LLM routing failed: {str(e)}", context=context, error=str(e))
            raise

        response_text = response.content if hasattr(response, 'content') else str(response)

        valid_nodes = {
            "databases", "schemas", "tables", "views", "stages", "external_locations",
            "streams", "pipes", "roles", "grants", "tags", "comments",
            "masking_policies", "udfs", "procedures", "sequences", "file_formats"
        }

        for node in valid_nodes:
            if node.lower() in response_text.lower():
                if metrics:
                    metrics.end_stage("artifact_router", success=True)
                return node

        if metrics:
            metrics.end_stage("artifact_router", success=True)
        return "tables"
    except Exception as e:
        if metrics:
            metrics.end_stage("artifact_router", success=False)
        raise
