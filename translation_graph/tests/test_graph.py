import unittest
import sys
import os

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.types import ArtifactBatch, TranslationResult
from utils.llm_utils import validate_node_requirements
from nodes.router import artifact_router
from nodes.tables_translation import translate_tables
from nodes.views_translation import translate_views
from nodes.schemas_translation import translate_schemas
from nodes.procedures_translation import translate_procedures
from nodes.roles_translation import translate_roles
from nodes.aggregator import aggregate_translations
from graph_builder import build_translation_graph


class TestTranslationGraph(unittest.TestCase):
    """Basic tests for the translation graph components."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_batch = ArtifactBatch(
            artifact_type="tables",
            items=["CREATE TABLE test (id INT)"],
            context={"source": "snowflake"}
        )

    def test_router_returns_valid_string(self):
        """Test that router returns a valid routing string."""
        result = artifact_router(self.test_batch)

        valid_routes = ["tables", "views", "schemas", "procedures", "roles"]
        self.assertIn(result, valid_routes)
        self.assertIsInstance(result, str)

    def test_translation_nodes_return_results(self):
        """Test that all translation nodes return proper TranslationResult objects."""
        translation_functions = [
            (translate_tables, "tables"),
            (translate_views, "views"),
            (translate_schemas, "schemas"),
            (translate_procedures, "procedures"),
            (translate_roles, "roles")
        ]

        for translate_func, expected_type in translation_functions:
            with self.subTest(node=expected_type):
                result = translate_func(self.test_batch)
                self.assertIsInstance(result, TranslationResult)
                self.assertEqual(result.artifact_type, expected_type)
                self.assertIsInstance(result.results, list)
                self.assertIsInstance(result.errors, list)
                self.assertIsInstance(result.metadata, dict)

    def test_aggregator_merges_results(self):
        """Test that aggregator correctly merges multiple results."""
        result1 = TranslationResult(
            artifact_type="tables",
            results=["table1", "table2"],
            errors=["error1"],
            metadata={"count": 2}
        )

        result2 = TranslationResult(
            artifact_type="views",
            results=["view1"],
            errors=[],
            metadata={"count": 1}
        )

        merged = aggregate_translations(result1, result2)

        # Check structure
        expected_keys = {"tables", "views", "schemas", "procedures", "roles", "metadata"}
        self.assertEqual(set(merged.keys()), expected_keys)

        # Check merged results
        self.assertEqual(merged["tables"], ["table1", "table2"])
        self.assertEqual(merged["views"], ["view1"])
        self.assertEqual(merged["schemas"], [])
        self.assertEqual(merged["procedures"], [])
        self.assertEqual(merged["roles"], [])

        # Check metadata
        self.assertEqual(merged["metadata"]["total_results"], 3)
        self.assertEqual(merged["metadata"]["errors"], ["error1"])

    def test_graph_runs_end_to_end(self):
        """Test that the full graph runs end-to-end."""
        graph = build_translation_graph()

        result = graph.run(self.test_batch)

        # Check that we get a result dictionary
        self.assertIsInstance(result, dict)

        # Check that all expected keys are present
        expected_keys = {"tables", "views", "schemas", "procedures", "roles", "metadata"}
        self.assertEqual(set(result.keys()), expected_keys)

        # Check metadata structure
        self.assertIn("total_results", result["metadata"])
        self.assertIn("errors", result["metadata"])
        self.assertIn("processing_stats", result["metadata"])

    def test_node_validation_structure(self):
        """Test that node validation returns expected structure."""
        validation_result = validate_node_requirements()

        # Check that we get expected keys
        expected_keys = {"llm_available", "environment_ready", "errors"}
        self.assertEqual(set(validation_result.keys()), expected_keys)

        # Check that errors is a list
        self.assertIsInstance(validation_result["errors"], list)

        # Check that boolean flags are present
        for key in ["llm_available", "environment_ready"]:
            self.assertIsInstance(validation_result[key], bool)


if __name__ == '__main__':
    unittest.main()
