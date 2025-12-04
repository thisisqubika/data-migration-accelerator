from typing import Any, Dict, Optional
from nodes.router import artifact_router
from nodes.tables_translation import translate_tables
from nodes.views_translation import translate_views
from nodes.schemas_translation import translate_schemas
from nodes.procedures_translation import translate_procedures
from nodes.roles_translation import translate_roles
from nodes.aggregator import aggregate_translations
from utils.types import ArtifactBatch
from utils.observability import initialize, finalize, get_observability
from utils.logger import LogLevel


class TranslationGraph:
    def __init__(self, run_id: Optional[str] = None, log_level: LogLevel = LogLevel.INFO, log_file: Optional[str] = None):
        """
        Initialize translation graph with observability.
        
        Args:
            run_id: Unique identifier for this run
            log_level: Minimum log level
            log_file: Optional path to log file
        """
        self.nodes = {
            "router": artifact_router,
            "translate_tables": translate_tables,
            "translate_views": translate_views,
            "translate_schemas": translate_schemas,
            "translate_procedures": translate_procedures,
            "translate_roles": translate_roles,
            "aggregate": aggregate_translations
        }
        
        # Initialize observability
        self.obs = initialize(run_id=run_id, log_level=log_level, log_file=log_file)
        self.logger = self.obs.get_logger("translation_graph")
        self.logger.info("Translation graph initialized", context={"run_id": run_id})

    def run(self, batch: ArtifactBatch) -> Dict[str, Any]:
        """
        Run the translation graph.
        
        Args:
            batch: Artifact batch to process
            
        Returns:
            Dictionary with translation results and metadata
        """
        self.logger.info("Starting translation graph execution", context={
            "artifact_type": batch.artifact_type,
            "batch_size": len(batch.items)
        })
        
        try:
            target_node = self.nodes["router"](batch)

            translation_functions = {
                "tables": self.nodes["translate_tables"],
                "views": self.nodes["translate_views"],
                "schemas": self.nodes["translate_schemas"],
                "procedures": self.nodes["translate_procedures"],
                "roles": self.nodes["translate_roles"]
            }

            if target_node not in translation_functions:
                raise ValueError(f"Unknown target node: {target_node}")

            translation_result = translation_functions[target_node](batch)
            final_result = self.nodes["aggregate"](translation_result)
            
            # Add observability summary to results
            summary = finalize()
            final_result["observability"] = summary
            
            self.logger.info("Translation graph execution completed", context={
                "artifact_type": batch.artifact_type,
                "success": True
            })
            
            return final_result
        except Exception as e:
            self.logger.error("Translation graph execution failed", context={
                "artifact_type": batch.artifact_type
            }, error=str(e))
            summary = finalize()
            raise


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
