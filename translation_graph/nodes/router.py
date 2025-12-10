from prompts.router_prompts import RouterPrompts
from config.ddl_config import get_config
from utils.types import ArtifactBatch
from utils.error_handler import handle_node_error
from utils.observability import get_observability
from utils.llm_utils import create_structured_llm
from utils.evaluation_models import RouterResponse


@handle_node_error("artifact_router")
def artifact_router(batch: ArtifactBatch) -> str:
    """
    Route artifacts to the appropriate translation node.
    
    If artifact_type is already set in the batch, returns it directly without LLM routing.
    Otherwise, uses LLM with structured outputs to determine the artifact type from DDL content.

    Args:
        batch: The artifact batch to route

    Returns:
        String indicating the target node: "databases", "schemas", "tables", "views",
        "stages", "external_locations", "streams", "pipes", "roles", "grants", "tags",
        "comments", "masking_policies", "udfs", "procedures", "file_formats"
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
        valid_nodes = {
            "databases", "schemas", "tables", "views", "stages", "external_locations",
            "streams", "pipes", "roles", "grants", "tags", "comments",
            "masking_policies", "udfs", "procedures", "file_formats"
        }
        
        if batch.artifact_type and batch.artifact_type in valid_nodes:
            if metrics:
                metrics.end_stage("artifact_router", success=True)
            return batch.artifact_type
        
        config = get_config()
        llm_config = config.get_llm_for_node("smart_router")
        structured_llm = create_structured_llm("smart_router", RouterResponse)
        prompt = RouterPrompts.create_prompt()

        ddl_content = "\n".join(batch.items) if batch.items else ""
        routing_prompt = f"{prompt}\n\nDDL Content:\n{ddl_content}"

        # Track AI call
        import time
        ai_start = time.time()
        try:
            response = structured_llm.invoke(routing_prompt)
            ai_latency = time.time() - ai_start
            
            if metrics:
                metrics.record_ai_call(
                    provider=llm_config.provider,
                    model=llm_config.model,
                    latency=ai_latency,
                    error=False
                )
            
            if isinstance(response, RouterResponse):
                artifact_type = response.artifact_type.lower().strip()
                if artifact_type in valid_nodes:
                    if metrics:
                        metrics.end_stage("artifact_router", success=True)
                    return artifact_type
            
            if metrics:
                metrics.end_stage("artifact_router", success=True)
            return "tables"
        except Exception as e:
            ai_latency = time.time() - ai_start
            if metrics:
                metrics.record_ai_call(
                    provider=llm_config.provider,
                    model=llm_config.model,
                    latency=ai_latency,
                    error=True
                )
            if logger:
                logger.error(f"LLM routing failed: {str(e)}", context=context, error=str(e))
            if metrics:
                metrics.end_stage("artifact_router", success=False)
            return "tables"
    except Exception as e:
        if metrics:
            metrics.end_stage("artifact_router", success=False)
        raise
