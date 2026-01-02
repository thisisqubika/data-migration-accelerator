"""
Evaluation and benchmarking module for SQL translation quality assessment.

This module provides tools for evaluating and benchmarking SQL translation models
using MLflow and LLM-as-a-judge patterns.
"""

from artifact_translation_package.evaluation.evaluation_dataset import (
    EvaluationDataset,
    load_evaluation_dataset
)
from artifact_translation_package.evaluation.databricks_sql_scorer import (
    DatabricksSQLComplianceScorer,
    create_compliance_scorer
)
from artifact_translation_package.evaluation.model_benchmark import (
    ModelConfig,
    ModelBenchmark,
    create_default_model_configs
)
from artifact_translation_package.evaluation.run_benchmark import (
    BenchmarkRunner,
    run_benchmark
)

__all__ = [
    "EvaluationDataset",
    "load_evaluation_dataset",
    "DatabricksSQLComplianceScorer",
    "create_compliance_scorer",
    "ModelConfig",
    "ModelBenchmark",
    "create_default_model_configs",
    "BenchmarkRunner",
    "run_benchmark"
]
