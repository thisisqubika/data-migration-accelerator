from typing import Any, Dict, List, Optional
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


class TranslationGraph:
    def __init__(self):
        self.nodes = {
            "router": artifact_router,
            "translate_databases": translate_databases,
            "translate_schemas": translate_schemas,
            "translate_tables": translate_tables,
            "translate_views": translate_views,
            "translate_stages": translate_stages,
            "translate_streams": translate_streams,
            "translate_pipes": translate_pipes,
            "translate_roles": translate_roles,
            "translate_grants": translate_grants,
            "translate_tags": translate_tags,
            "translate_comments": translate_comments,
            "translate_masking_policies": translate_masking_policies,
            "translate_udfs": translate_udfs,
            "translate_procedures": translate_procedures,
            "translate_sequences": translate_sequences,
            "translate_file_formats": translate_file_formats,
            "translate_external_locations": translate_external_locations,
            "aggregate": aggregate_translations
        }

    def run(self, batch: ArtifactBatch) -> Dict[str, Any]:
        target_node = self.nodes["router"](batch)

        translation_functions = {
            "databases": self.nodes["translate_databases"],
            "schemas": self.nodes["translate_schemas"],
            "tables": self.nodes["translate_tables"],
            "views": self.nodes["translate_views"],
            "stages": self.nodes["translate_stages"],
            "external_locations": self.nodes["translate_external_locations"],
            "streams": self.nodes["translate_streams"],
            "pipes": self.nodes["translate_pipes"],
            "roles": self.nodes["translate_roles"],
            "grants": self.nodes["translate_grants"],
            "tags": self.nodes["translate_tags"],
            "comments": self.nodes["translate_comments"],
            "masking_policies": self.nodes["translate_masking_policies"],
            "udfs": self.nodes["translate_udfs"],
            "procedures": self.nodes["translate_procedures"],
            "sequences": self.nodes["translate_sequences"],
            "file_formats": self.nodes["translate_file_formats"]
        }

        if target_node not in translation_functions:
            raise ValueError(f"Unknown target node: {target_node}")

        translation_result = translation_functions[target_node](batch)
        final_result = self.nodes["aggregate"](translation_result)

        return final_result

    def run_batches(self, batches: List[ArtifactBatch]) -> Dict[str, Any]:
        """
        Process multiple batches and aggregate results.
        
        Args:
            batches: List of ArtifactBatch objects to process
            
        Returns:
            Aggregated translation results
        """
        translation_results: List[TranslationResult] = []
        
        for batch in batches:
            target_node = self.nodes["router"](batch)
            
            translation_functions = {
                "databases": self.nodes["translate_databases"],
                "schemas": self.nodes["translate_schemas"],
                "tables": self.nodes["translate_tables"],
                "views": self.nodes["translate_views"],
                "stages": self.nodes["translate_stages"],
                "external_locations": self.nodes["translate_external_locations"],
                "streams": self.nodes["translate_streams"],
                "pipes": self.nodes["translate_pipes"],
                "roles": self.nodes["translate_roles"],
                "grants": self.nodes["translate_grants"],
                "tags": self.nodes["translate_tags"],
                "comments": self.nodes["translate_comments"],
                "masking_policies": self.nodes["translate_masking_policies"],
                "udfs": self.nodes["translate_udfs"],
                "procedures": self.nodes["translate_procedures"],
                "sequences": self.nodes["translate_sequences"],
                "file_formats": self.nodes["translate_file_formats"]
            }
            
            if target_node not in translation_functions:
                raise ValueError(f"Unknown target node: {target_node}")
            
            translation_result = translation_functions[target_node](batch)
            translation_results.append(translation_result)
        
        if translation_results:
            final_result = self.nodes["aggregate"](*translation_results)
            return final_result
        
        return {
            "metadata": {
                "total_results": 0,
                "errors": [],
                "processing_stats": {}
            }
        }


def build_translation_graph() -> TranslationGraph:
    return TranslationGraph()
