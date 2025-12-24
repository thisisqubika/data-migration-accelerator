from typing import Any, Dict, List, Optional, Annotated, TypedDict
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableConfig
import time

from artifact_translation_package.nodes.router import artifact_router
from artifact_translation_package.nodes.tables_translation import translate_tables
from artifact_translation_package.nodes.views_translation import translate_views
from artifact_translation_package.nodes.schemas_translation import translate_schemas
from artifact_translation_package.nodes.procedures_translation import translate_procedures
from artifact_translation_package.nodes.roles_translation import translate_roles
from artifact_translation_package.nodes.database_translation import translate_databases
from artifact_translation_package.nodes.stages_translation import translate_stages
from artifact_translation_package.nodes.streams_translation import translate_streams
from artifact_translation_package.nodes.pipes_translation import translate_pipes
from artifact_translation_package.nodes.tags_translation import translate_tags
from artifact_translation_package.nodes.comments_translation import translate_comments
from artifact_translation_package.nodes.masking_policies_translation import translate_masking_policies
from artifact_translation_package.nodes.grants_translation import translate_grants
from artifact_translation_package.nodes.udfs_translation import translate_udfs
from artifact_translation_package.nodes.external_locations_translation import translate_external_locations
from artifact_translation_package.nodes.aggregator import aggregate_translations
from artifact_translation_package.nodes.syntax_evaluation import evaluate_batch
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.observability import initialize, finalize, get_observability
from artifact_translation_package.utils.logger import LogLevel


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
    result = translate_databases(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_schemas_node(state: TranslationState) -> TranslationState:
    """Translate schema artifacts."""
    if not state["batch"]:
        return state
    result = translate_schemas(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_tables_node(state: TranslationState) -> TranslationState:
    """Translate table artifacts."""
    if not state["batch"]:
        return state
    result = translate_tables(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_views_node(state: TranslationState) -> TranslationState:
    """Translate view artifacts."""
    if not state["batch"]:
        return state
    result = translate_views(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_stages_node(state: TranslationState) -> TranslationState:
    """Translate stage artifacts."""
    if not state["batch"]:
        return state
    result = translate_stages(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_external_locations_node(state: TranslationState) -> TranslationState:
    """Translate external location artifacts."""
    if not state["batch"]:
        return state
    result = translate_external_locations(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_streams_node(state: TranslationState) -> TranslationState:
    """Translate stream artifacts."""
    if not state["batch"]:
        return state
    result = translate_streams(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_pipes_node(state: TranslationState) -> TranslationState:
    """Translate pipe artifacts."""
    if not state["batch"]:
        return state
    result = translate_pipes(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_roles_node(state: TranslationState) -> TranslationState:
    """Translate role artifacts."""
    if not state["batch"]:
        return state
    result = translate_roles(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_grants_node(state: TranslationState) -> TranslationState:
    """Translate grant artifacts."""
    if not state["batch"]:
        return state
    result = translate_grants(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_tags_node(state: TranslationState) -> TranslationState:
    """Translate tag artifacts."""
    if not state["batch"]:
        return state
    result = translate_tags(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_comments_node(state: TranslationState) -> TranslationState:
    """Translate comment artifacts."""
    if not state["batch"]:
        return state
    result = translate_comments(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_masking_policies_node(state: TranslationState) -> TranslationState:
    """Translate masking policy artifacts."""
    if not state["batch"]:
        return state
    result = translate_masking_policies(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_udfs_node(state: TranslationState) -> TranslationState:
    """Translate UDF artifacts."""
    if not state["batch"]:
        return state
    result = translate_udfs(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_procedures_node(state: TranslationState) -> TranslationState:
    """Translate procedure artifacts."""
    if not state["batch"]:
        return state
    result = translate_procedures(state["batch"])
    return {**state, "results": state["results"] + [result]}







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
        # file_formats are intentionally excluded from the runtime graph
        # to disable their execution while keeping the translation code present.
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
                # file_formats mapping intentionally omitted
                    "aggregator": "aggregator",
            }
        )

        # Add edges from all translation nodes to evaluation, then to aggregator
        translation_nodes = [
            "translate_databases", "translate_schemas", "translate_tables", "translate_views",
            "translate_stages", "translate_external_locations", "translate_streams", "translate_pipes",
            "translate_roles", "translate_grants", "translate_tags", "translate_comments",
            "translate_masking_policies", "translate_udfs", "translate_procedures",
                            # "translate_file_formats" intentionally excluded
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
            summary = finalize()
            raise

    def _initialize_merged_result(self) -> Dict[str, Any]:
        """
        Initialize empty merged result structure.
        
        Returns:
            Dictionary with empty result structure
        """
        return {
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
            "metadata": {
                "total_results": 0,
                "errors": [],
                "processing_stats": {}
            }
        }

    def _merge_result_into(
        self,
        merged_result: Dict[str, Any],
        result: Dict[str, Any]
    ) -> None:
        """
        Merge a single result into the merged result structure.
        
        Args:
            merged_result: The merged result dictionary to update
            result: A single result dictionary to merge
        """
        for key, value in result.items():
            if key == "metadata":
                merged_result["metadata"]["total_results"] += result["metadata"].get("total_results", 0)
                merged_result["metadata"]["errors"].extend(result["metadata"].get("errors", []))
                merged_result["metadata"]["processing_stats"].update(result["metadata"].get("processing_stats", {}))
            elif key == "observability":
                # Merge observability data
                if "observability" not in merged_result:
                    merged_result["observability"] = {
                        "run_id": value.get("run_id"),
                        "total_duration": 0,
                        "total_errors": 0,
                        "total_warnings": 0,
                        "total_retries": 0,
                        "artifact_counts": {},
                        "stages": {},
                        "ai_metrics": {}
                    }
                
                obs = merged_result["observability"]
                
                # Aggregate artifact_counts
                for artifact_type, count in value.get("artifact_counts", {}).items():
                    obs["artifact_counts"][artifact_type] = obs["artifact_counts"].get(artifact_type, 0) + count
                
                # Aggregate total_errors, total_warnings, total_retries
                obs["total_errors"] += value.get("total_errors", 0)
                obs["total_warnings"] += value.get("total_warnings", 0)
                obs["total_retries"] += value.get("total_retries", 0)
                
                # Merge stages (aggregate items_processed and error_count)
                for stage_name, stage_data in value.get("stages", {}).items():
                    if stage_name not in obs["stages"]:
                        obs["stages"][stage_name] = stage_data
                    else:
                        # Aggregate items_processed and error_count
                        obs["stages"][stage_name]["items_processed"] += stage_data.get("items_processed", 0)
                        obs["stages"][stage_name]["error_count"] += stage_data.get("error_count", 0)
                
                # Merge ai_metrics
                for ai_key, ai_data in value.get("ai_metrics", {}).items():
                    if ai_key not in obs["ai_metrics"]:
                        obs["ai_metrics"][ai_key] = ai_data
                    else:
                        # Aggregate AI metrics
                        obs["ai_metrics"][ai_key]["call_count"] += ai_data.get("call_count", 0)
                        obs["ai_metrics"][ai_key]["total_latency"] += ai_data.get("total_latency", 0)
                        obs["ai_metrics"][ai_key]["errors"] += ai_data.get("errors", 0)
                        # Recalculate average latency
                        if obs["ai_metrics"][ai_key]["call_count"] > 0:
                            obs["ai_metrics"][ai_key]["average_latency"] = (
                                obs["ai_metrics"][ai_key]["total_latency"] /
                                obs["ai_metrics"][ai_key]["call_count"]
                            )
            elif key in merged_result:
                merged_result[key].extend(value)

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
        start_time = time.time()

        for batch in batches:
            result = self.run(batch)
            if result:
                all_results.append(result)

        if all_results:
            merged_result = self._initialize_merged_result()
            
            for result in all_results:
                self._merge_result_into(merged_result, result)
            
            # Calculate total duration
            end_time = time.time()
            if "observability" in merged_result:
                merged_result["observability"]["total_duration"] = end_time - start_time

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
