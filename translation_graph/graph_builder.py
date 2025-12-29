from typing import Any, Dict, List, Optional, Annotated, TypedDict
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableConfig

from nodes.router import artifact_router
from nodes.tables_translation import translate_tables
from nodes.views_translation import translate_views
from nodes.schemas_translation import translate_schemas
from nodes.procedures_translation import translate_procedures
from nodes.roles_translation import translate_roles
from nodes.database_translation import translate_databases
from nodes.stages_translation import translate_stages
from nodes.streams_translation import translate_streams
from nodes.pipes_translation import translate_pipes
from nodes.tags_translation import translate_tags
from nodes.comments_translation import translate_comments
from nodes.masking_policies_translation import translate_masking_policies
from nodes.grants_translation import translate_grants
from nodes.udfs_translation import translate_udfs
from nodes.file_formats_translation import translate_file_formats
from nodes.external_locations_translation import translate_external_locations
from nodes.aggregator import aggregate_translations
from nodes.syntax_evaluation import evaluate_batch
from utils.types import ArtifactBatch, TranslationResult
from utils.observability import initialize, finalize, get_observability
from utils.logger import LogLevel


class TranslationState(TypedDict):
    """State for the translation graph execution."""
    batch: Optional[ArtifactBatch]
    results: List[TranslationResult]
    final_result: Optional[Dict[str, Any]]
    errors: List[str]
    target_node: Optional[str]
    evaluation_results: List[Dict[str, Any]]


def router_node(state: TranslationState) -> TranslationState:
    """Route the batch to the appropriate translation node."""
    if not state["batch"]:
        return {**state, "target_node": None, "errors": state["errors"] + ["No batch provided"]}

    target_node = artifact_router(state["batch"])
    return {**state, "target_node": target_node}


def translate_databases_node(state: TranslationState) -> TranslationState:
    """Translate database artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_databases", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_databases(state["batch"])
        if metrics:
            metrics.end_stage("translate_databases", success=True, items_processed=len(batch.items))
            metrics.record_artifact("databases", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_databases", success=False)
        raise


def translate_schemas_node(state: TranslationState) -> TranslationState:
    """Translate schema artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_schemas", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_schemas(state["batch"])
        if metrics:
            metrics.end_stage("translate_schemas", success=True, items_processed=len(batch.items))
            metrics.record_artifact("schemas", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_schemas", success=False)
        raise


def translate_tables_node(state: TranslationState) -> TranslationState:
    """Translate table artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_tables", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_tables(state["batch"])
        if metrics:
            metrics.end_stage("translate_tables", success=True, items_processed=len(batch.items))
            metrics.record_artifact("tables", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_tables", success=False)
        raise


def translate_views_node(state: TranslationState) -> TranslationState:
    """Translate view artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_views", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_views(state["batch"])
        if metrics:
            metrics.end_stage("translate_views", success=True, items_processed=len(batch.items))
            metrics.record_artifact("views", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_views", success=False)
        raise


def translate_stages_node(state: TranslationState) -> TranslationState:
    """Translate stage artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_stages", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_stages(state["batch"])
        if metrics:
            metrics.end_stage("translate_stages", success=True, items_processed=len(batch.items))
            metrics.record_artifact("stages", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_stages", success=False)
        raise


def translate_external_locations_node(state: TranslationState) -> TranslationState:
    """Translate external location artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_external_locations", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_external_locations(state["batch"])
        if metrics:
            metrics.end_stage("translate_external_locations", success=True, items_processed=len(batch.items))
            metrics.record_artifact("external_locations", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_external_locations", success=False)
        raise


def translate_streams_node(state: TranslationState) -> TranslationState:
    """Translate stream artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_streams", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_streams(state["batch"])
        if metrics:
            metrics.end_stage("translate_streams", success=True, items_processed=len(batch.items))
            metrics.record_artifact("streams", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_streams", success=False)
        raise


def translate_pipes_node(state: TranslationState) -> TranslationState:
    """Translate pipe artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_pipes", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_pipes(state["batch"])
        if metrics:
            metrics.end_stage("translate_pipes", success=True, items_processed=len(batch.items))
            metrics.record_artifact("pipes", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_pipes", success=False)
        raise


def translate_roles_node(state: TranslationState) -> TranslationState:
    """Translate role artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_roles", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_roles(state["batch"])
        if metrics:
            metrics.end_stage("translate_roles", success=True, items_processed=len(batch.items))
            metrics.record_artifact("roles", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_roles", success=False)
        raise


def translate_grants_node(state: TranslationState) -> TranslationState:
    """Translate grant artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_grants", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_grants(state["batch"])
        if metrics:
            metrics.end_stage("translate_grants", success=True, items_processed=len(batch.items))
            metrics.record_artifact("grants", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_grants", success=False)
        raise


def translate_tags_node(state: TranslationState) -> TranslationState:
    """Translate tag artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_tags", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_tags(state["batch"])
        if metrics:
            metrics.end_stage("translate_tags", success=True, items_processed=len(batch.items))
            metrics.record_artifact("tags", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_tags", success=False)
        raise


def translate_comments_node(state: TranslationState) -> TranslationState:
    """Translate comment artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_comments", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_comments(state["batch"])
        if metrics:
            metrics.end_stage("translate_comments", success=True, items_processed=len(batch.items))
            metrics.record_artifact("comments", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_comments", success=False)
        raise


def translate_masking_policies_node(state: TranslationState) -> TranslationState:
    """Translate masking policy artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_masking_policies", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_masking_policies(state["batch"])
        if metrics:
            metrics.end_stage("translate_masking_policies", success=True, items_processed=len(batch.items))
            metrics.record_artifact("masking_policies", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_masking_policies", success=False)
        raise


def translate_udfs_node(state: TranslationState) -> TranslationState:
    """Translate UDF artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_udfs", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_udfs(state["batch"])
        if metrics:
            metrics.end_stage("translate_udfs", success=True, items_processed=len(batch.items))
            metrics.record_artifact("udfs", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_udfs", success=False)
        raise


def translate_procedures_node(state: TranslationState) -> TranslationState:
    """Translate procedure artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_procedures", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_procedures(state["batch"])
        if metrics:
            metrics.end_stage("translate_procedures", success=True, items_processed=len(batch.items))
            metrics.record_artifact("procedures", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_procedures", success=False)
        raise


def translate_file_formats_node(state: TranslationState) -> TranslationState:
    """Translate file format artifacts."""
    if not state["batch"]:
        return state
    
    obs = get_observability()
    metrics = obs.get_metrics() if obs else None
    batch = state["batch"]
    
    if metrics:
        metrics.start_stage("translate_file_formats", {
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
    
    try:
        result = translate_file_formats(state["batch"])
        if metrics:
            metrics.end_stage("translate_file_formats", success=True, items_processed=len(batch.items))
            metrics.record_artifact("file_formats", count=len(batch.items))
        return {**state, "results": state["results"] + [result]}
    except Exception as e:
        if metrics:
            metrics.end_stage("translate_file_formats", success=False)
        raise


def evaluation_node(state: TranslationState) -> TranslationState:
    """
    Evaluate the last translation result for SQL syntax validity.
    
    Runs after each translation node to check if the SQL syntax is valid.
    Always persists validation results to a file.
    """
    if not state["batch"] or not state["results"]:
        return state
    
    last_result = state["results"][-1]
    all_valid, persisted_file, validation_result = evaluate_batch(state["batch"], last_result)
    
    evaluation_results = state.get("evaluation_results", [])
    
    if not all_valid:
        evaluation_result_info = {
            "batch": {
                "artifact_type": state["batch"].artifact_type,
                "items": state["batch"].items,
                "context": state["batch"].context
            },
            "translation_result": {
                "artifact_type": last_result.artifact_type,
                "results": last_result.results,
                "errors": last_result.errors,
                "metadata": last_result.metadata
            },
            "validation_result": validation_result.model_dump() if hasattr(validation_result, 'model_dump') else validation_result,
            "persisted_file": persisted_file
        }
        evaluation_results = evaluation_results + [evaluation_result_info]
    
    return {
        **state,
        "evaluation_results": evaluation_results
    }


def aggregator_node(state: TranslationState) -> TranslationState:
    """Aggregate all translation results into final output."""
    if not state["results"]:
        final_result = {
            "metadata": {
                "total_results": 0,
                "errors": state["errors"],
                "processing_stats": {},
                "evaluation_results_count": len(state.get("evaluation_results", []))
            }
        }
    else:
        evaluation_results = state.get("evaluation_results", [])
        final_result = aggregate_translations(*state["results"], evaluation_results=evaluation_results)

    return {**state, "final_result": final_result}


def route_to_translation_node(state: TranslationState) -> str:
    """Route to the appropriate translation node based on target_node."""
    target_node = state.get("target_node")
    if target_node:
        return target_node
    return "aggregator"  # Default fallback


class TranslationGraph:
    def __init__(self, run_id: Optional[str] = None, log_level: LogLevel = LogLevel.INFO, log_file: Optional[str] = None):
        """
        Initialize translation graph with observability.
        
        Args:
            run_id: Unique identifier for this run
            log_level: Minimum log level
            log_file: Optional path to log file
        """
        # Initialize observability
        self.obs = initialize(run_id=run_id, log_level=log_level, log_file=log_file)
        self.logger = self.obs.get_logger("translation_graph")
        self.logger.info("Translation graph initialized", context={"run_id": run_id})
        
        # Create the StateGraph
        self.graph = StateGraph(TranslationState)

        # Add nodes
        self.graph.add_node("router", router_node)
        self.graph.add_node("translate_databases", translate_databases_node)
        self.graph.add_node("translate_schemas", translate_schemas_node)
        self.graph.add_node("translate_tables", translate_tables_node)
        self.graph.add_node("translate_views", translate_views_node)
        self.graph.add_node("translate_stages", translate_stages_node)
        self.graph.add_node("translate_external_locations", translate_external_locations_node)
        self.graph.add_node("translate_streams", translate_streams_node)
        self.graph.add_node("translate_pipes", translate_pipes_node)
        self.graph.add_node("translate_roles", translate_roles_node)
        self.graph.add_node("translate_grants", translate_grants_node)
        self.graph.add_node("translate_tags", translate_tags_node)
        self.graph.add_node("translate_comments", translate_comments_node)
        self.graph.add_node("translate_masking_policies", translate_masking_policies_node)
        self.graph.add_node("translate_udfs", translate_udfs_node)
        self.graph.add_node("translate_procedures", translate_procedures_node)
        self.graph.add_node("translate_file_formats", translate_file_formats_node)
        self.graph.add_node("evaluation", evaluation_node)
        self.graph.add_node("aggregator", aggregator_node)

        # Set entry point
        self.graph.set_entry_point("router")

        # Add conditional edges from router to translation nodes
        self.graph.add_conditional_edges(
            "router",
            route_to_translation_node,
            {
                "databases": "translate_databases",
                "schemas": "translate_schemas",
                "tables": "translate_tables",
                "views": "translate_views",
                "stages": "translate_stages",
                "external_locations": "translate_external_locations",
                "streams": "translate_streams",
                "pipes": "translate_pipes",
                "roles": "translate_roles",
                "grants": "translate_grants",
                "tags": "translate_tags",
                "comments": "translate_comments",
                "masking_policies": "translate_masking_policies",
                "udfs": "translate_udfs",
                "procedures": "translate_procedures",
                "file_formats": "translate_file_formats",
            }
        )

        # Add edges from all translation nodes to evaluation, then to aggregator
        translation_nodes = [
            "translate_databases", "translate_schemas", "translate_tables", "translate_views",
            "translate_stages", "translate_external_locations", "translate_streams", "translate_pipes",
            "translate_roles", "translate_grants", "translate_tags", "translate_comments",
            "translate_masking_policies", "translate_udfs", "translate_procedures",
            "translate_file_formats"
        ]

        for node in translation_nodes:
            self.graph.add_edge(node, "evaluation")
        
        self.graph.add_edge("evaluation", "aggregator")

        # Add edge from aggregator to END
        self.graph.add_edge("aggregator", END)

        # Compile the graph
        self.compiled_graph = self.graph.compile()

    def run(self, batch: ArtifactBatch) -> Dict[str, Any]:
        """Process a single batch through the translation graph."""
        self.logger.info("Starting translation graph execution", context={
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
        
        try:
            initial_state: TranslationState = {
            "batch": batch,
            "results": [],
            "final_result": None,
            "errors": [],
            "target_node": None,
            "evaluation_results": []
        }

            final_state = self.compiled_graph.invoke(initial_state)
            result = final_state["final_result"] or {}
            
            # Add observability summary to results
            summary = finalize()
            result["observability"] = summary
            
            self.logger.info("Translation graph execution completed", context={
                "artifact_type": batch.artifact_type,
                "success": True
            })
            
            return result
        except Exception as e:
            self.logger.error("Translation graph execution failed", context={
                "artifact_type": batch.artifact_type
            }, error=str(e))
            raise

    def run_batches(self, batches: List[ArtifactBatch]) -> Dict[str, Any]:
        """
        Process multiple batches and aggregate results.

        Args:
            batches: List of ArtifactBatch objects to process

        Returns:
            Aggregated translation results
        """
        self.logger.info("Starting batch processing", context={"batch_count": len(batches)})
        
        all_results = []

        for batch in batches:
            result = self.run(batch)
            if result:
                all_results.append(result)

        if all_results:
            # Merge all results
            merged_result = {
                "databases": [],
                "schemas": [],
                "tables": [],
                "views": [],
                "stages": [],
                "external_locations": [],
                "streams": [],
                "pipes": [],
                "roles": [],
                "grants": [],
                "tags": [],
                "comments": [],
                "masking_policies": [],
                "udfs": [],
                "procedures": [],
                "file_formats": [],
                "metadata": {
                    "total_results": 0,
                    "errors": [],
                    "processing_stats": {}
                }
            }

            for result in all_results:
                for key, value in result.items():
                    if key == "metadata":
                        merged_result["metadata"]["total_results"] += result["metadata"].get("total_results", 0)
                        merged_result["metadata"]["errors"].extend(result["metadata"].get("errors", []))
                        merged_result["metadata"]["processing_stats"].update(result["metadata"].get("processing_stats", {}))
                    elif key == "observability":
                        # Skip individual observability summaries, will add merged one at the end
                        pass
                    elif key in merged_result:
                        merged_result[key].extend(value)

            # Finalize observability after all batches are processed
            summary = finalize()
            merged_result["observability"] = summary

            self.logger.info("Batch processing completed", context={"total_batches": len(batches)})
            return merged_result

        return {
            "metadata": {
                "total_results": 0,
                "errors": [],
                "processing_stats": {}
            }
        }


def build_translation_graph(
    run_id: Optional[str] = None,
    log_level: LogLevel = LogLevel.INFO,
    log_file: Optional[str] = None
) -> TranslationGraph:
    """
    Build translation graph with observability.
    
    Args:
        run_id: Unique identifier for this run
        log_level: Minimum log level
        log_file: Optional path to log file
        
    Returns:
        TranslationGraph instance
    """
    return TranslationGraph(run_id=run_id, log_level=log_level, log_file=log_file)
