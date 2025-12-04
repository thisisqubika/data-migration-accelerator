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
from nodes.sequences_translation import translate_sequences
from nodes.file_formats_translation import translate_file_formats
from nodes.external_locations_translation import translate_external_locations
from nodes.aggregator import aggregate_translations
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


def translate_sequences_node(state: TranslationState) -> TranslationState:
    """Translate sequence artifacts."""
    if not state["batch"]:
        return state
    result = translate_sequences(state["batch"])
    return {**state, "results": state["results"] + [result]}


def translate_file_formats_node(state: TranslationState) -> TranslationState:
    """Translate file format artifacts."""
    if not state["batch"]:
        return state
    result = translate_file_formats(state["batch"])
    return {**state, "results": state["results"] + [result]}


def aggregator_node(state: TranslationState) -> TranslationState:
    """Aggregate all translation results into final output."""
    if not state["results"]:
        final_result = {
            "metadata": {
                "total_results": 0,
                "errors": state["errors"],
                "processing_stats": {}
            }
        }
    else:
        final_result = aggregate_translations(*state["results"])

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
        self.graph.add_node("translate_sequences", translate_sequences_node)
        self.graph.add_node("translate_file_formats", translate_file_formats_node)
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
                "sequences": "translate_sequences",
                "file_formats": "translate_file_formats",
            }
        )

        # Add edges from all translation nodes to aggregator
        translation_nodes = [
            "translate_databases", "translate_schemas", "translate_tables", "translate_views",
            "translate_stages", "translate_external_locations", "translate_streams", "translate_pipes",
            "translate_roles", "translate_grants", "translate_tags", "translate_comments",
            "translate_masking_policies", "translate_udfs", "translate_procedures",
            "translate_sequences", "translate_file_formats"
        ]

        for node in translation_nodes:
            self.graph.add_edge(node, "aggregator")

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
                "target_node": None
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
                "sequences": [],
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
                        # Keep only the last observability summary
                        merged_result["observability"] = value
                    elif key in merged_result:
                        merged_result[key].extend(value)

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
