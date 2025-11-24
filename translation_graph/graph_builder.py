from typing import Any, Dict
from nodes.router import artifact_router
from nodes.tables_translation import translate_tables
from nodes.views_translation import translate_views
from nodes.schemas_translation import translate_schemas
from nodes.procedures_translation import translate_procedures
from nodes.roles_translation import translate_roles
from nodes.aggregator import aggregate_translations
from utils.types import ArtifactBatch


class TranslationGraph:
    def __init__(self):
        self.nodes = {
            "router": artifact_router,
            "translate_tables": translate_tables,
            "translate_views": translate_views,
            "translate_schemas": translate_schemas,
            "translate_procedures": translate_procedures,
            "translate_roles": translate_roles,
            "aggregate": aggregate_translations
        }

    def run(self, batch: ArtifactBatch) -> Dict[str, Any]:
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

        return final_result


def build_translation_graph() -> TranslationGraph:
    return TranslationGraph()
