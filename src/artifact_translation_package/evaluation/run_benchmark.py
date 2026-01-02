"""
Main benchmark orchestrator for SQL translation model evaluation.

This module provides the BenchmarkRunner class that coordinates the entire
benchmarking pipeline using MLflow for experiment tracking.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

import mlflow

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
    create_model_benchmark,
    create_default_model_configs
)
from artifact_translation_package.utils.types import ArtifactBatch
from artifact_translation_package.config.ddl_config import get_config

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Orchestrates SQL translation model benchmarking using MLflow."""
    
    def __init__(
        self,
        experiment_name: str = "sql-translation-benchmark",
        mlflow_tracking_uri: Optional[str] = None
    ):
        self.experiment_name = experiment_name
        self.mlflow_tracking_uri = mlflow_tracking_uri
        self._setup_mlflow()
        
    def _setup_mlflow(self):
        """Setup MLflow experiment and tracking."""
        if self.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.mlflow_tracking_uri)
            logger.info(f"Set MLflow tracking URI to: {self.mlflow_tracking_uri}")
        
        # mlflow.set_experiment() will create the experiment if it doesn't exist
        try:
            mlflow.set_experiment(self.experiment_name)
            logger.info(f"MLflow experiment ready: {self.experiment_name}")
        except Exception as e:
            logger.error(f"Failed to setup MLflow experiment '{self.experiment_name}': {e}")
            logger.error("Make sure you have the correct permissions in Databricks workspace")
            raise RuntimeError(f"Could not setup MLflow experiment: {e}") from e
    
    def run_benchmark(
        self,
        artifact_type: str,
        dataset_source: Optional[str] = None,
        model_configs: Optional[List[ModelConfig]] = None,
        batch_size: int = 10,
        judge_endpoint: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Run complete benchmark comparing multiple models.
        
        Args:
            artifact_type: Type of artifact to benchmark (tables, views, etc.)
            dataset_source: Optional path to dataset file
            model_configs: Optional list of model configs (uses defaults if None)
            batch_size: Batch size for translation
            judge_endpoint: Optional specific LLM endpoint for judging
            
        Returns:
            DataFrame with benchmark results
        """
        logger.info(f"Starting benchmark for artifact type: {artifact_type}")
        
        # Load evaluation dataset
        dataset = EvaluationDataset(
            artifact_type=artifact_type,
            source_file=dataset_source
        )
        dataset.load()
        
        logger.info(f"Loaded {len(dataset)} artifacts for evaluation")
        
        # Create batches for translation
        batches = dataset.to_batches(batch_size=batch_size)
        
        # Setup model benchmark
        benchmark = ModelBenchmark(artifact_type)
        
        if model_configs:
            for config in model_configs:
                benchmark.add_model_config(config)
        else:
            # Use default configurations
            for config in create_default_model_configs():
                benchmark.add_model_config(config)
        
        logger.info(f"Benchmarking {len(benchmark)} model configurations")
        
        # Create scorer for evaluation
        scorer = create_compliance_scorer(llm_endpoint=judge_endpoint)
        
        # Ensure experiment is set before creating runs
        mlflow.set_experiment(self.experiment_name)
        
        # Create parent run for comparison
        parent_run_name = f"benchmark_{artifact_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with mlflow.start_run(run_name=parent_run_name) as parent_run:
            # Log benchmark-level parameters
            mlflow.log_params({
                "artifact_type": artifact_type,
                "num_models": len(benchmark),
                "batch_size": batch_size,
                "judge_endpoint": judge_endpoint or "databricks-llama-4-maverick"
            })
            
            # Run benchmark for each model in NESTED runs
            results = []
            for model_config in benchmark.get_model_configs():
                logger.info(f"\n{'='*60}")
                logger.info(f"Benchmarking: {model_config.name}")
                logger.info(f"{'='*60}")
                
                result = self._run_single_model_benchmark(
                    model_config=model_config,
                    batches=batches,
                    benchmark=benchmark,
                    scorer=scorer,
                    artifact_type=artifact_type,
                    parent_run_id=parent_run.info.run_id
                )
                results.append(result)
        
        # Compile results into DataFrame
        results_df = pd.DataFrame(results)
        
        logger.info(f"\n{'='*60}")
        logger.info("Benchmark Summary")
        logger.info(f"{'='*60}")
        logger.info(f"\n{results_df.to_string()}")
        
        return results_df
    
    def _run_single_model_benchmark(
        self,
        model_config: ModelConfig,
        batches: List[ArtifactBatch],
        benchmark: ModelBenchmark,
        scorer: DatabricksSQLComplianceScorer,
        artifact_type: str,
        parent_run_id: str
    ) -> Dict[str, Any]:
        """
        Run benchmark for a single model configuration as a nested run.
        
        Args:
            model_config: Model configuration to benchmark
            batches: Artifact batches to translate
            benchmark: ModelBenchmark instance
            scorer: Compliance scorer
            artifact_type: Type of artifact
            parent_run_id: Parent MLflow run ID for nesting
            
        Returns:
            Dictionary with benchmark metrics
        """
        # Create NESTED run for this specific model
        with mlflow.start_run(run_name=model_config.name, nested=True):
            # Log model-specific parameters
            mlflow.log_params({
                "endpoint": model_config.endpoint,
                "temperature": model_config.temperature,
                "max_tokens": model_config.max_tokens,
            })
            
            # Run translation
            start_time = datetime.now()
            translation_result = benchmark.run_translation(batches, model_config)
            end_time = datetime.now()
            translation_duration = (end_time - start_time).total_seconds()
            
            # Extract SQL results
            sql_results = translation_result.get(artifact_type, [])
            total_results = len(sql_results)
            
            # Evaluate each SQL statement
            compliance_scores = []
            best_practices_scores = []
            syntax_valid_count = 0
            compliant_count = 0
            all_issues = []
            
            eval_start_time = datetime.now()
            for idx, sql_statement in enumerate(sql_results):
                try:
                    eval_result = scorer.evaluate_sql(sql_statement)
                    compliance_scores.append(eval_result.get("compliance_score", 0.0))
                    best_practices_scores.append(eval_result.get("best_practices_score", 0.0))
                    
                    # Track metrics
                    if eval_result.get("syntax_valid", False):
                        syntax_valid_count += 1
                    if eval_result.get("is_compliant", False):
                        compliant_count += 1
                        
                    # Collect issues for summary
                    for issue in eval_result.get("issues", []):
                        issue["sql_idx"] = idx
                        all_issues.append(issue)
                        
                    # Log individual evaluation (sample first 5)
                    if idx < 5:
                        mlflow.log_dict(
                            eval_result,
                            f"evaluations/sql_{idx}_evaluation.json"
                        )
                        
                except Exception as e:
                    logger.error(f"Error evaluating SQL {idx}: {str(e)}")
                    compliance_scores.append(0.0)
            
            eval_end_time = datetime.now()
            evaluation_duration = (eval_end_time - eval_start_time).total_seconds()
            
            # Calculate metrics
            avg_compliance_score = (
                sum(compliance_scores) / len(compliance_scores)
                if compliance_scores else 0.0
            )
            avg_best_practices_score = (
                sum(best_practices_scores) / len(best_practices_scores)
                if best_practices_scores else 0.0
            )
            syntax_valid_pct = (syntax_valid_count / total_results * 100) if total_results > 0 else 0.0
            compliant_pct = (compliant_count / total_results * 100) if total_results > 0 else 0.0
            
            # Log metrics with STANDARD names (no prefix) for grouped comparison
            mlflow.log_metrics({
                "avg_compliance_score": avg_compliance_score,
                "avg_best_practices_score": avg_best_practices_score,
                "syntax_valid_pct": syntax_valid_pct,
                "compliant_pct": compliant_pct,
                "total_sql": float(total_results),
                "translation_duration_sec": translation_duration,
                "evaluation_duration_sec": evaluation_duration,
                "total_duration_sec": translation_duration + evaluation_duration
            })
            
            # Log translation result as artifact
            mlflow.log_dict(translation_result, "translation_result.json")
            
            logger.info(f"Average compliance score: {avg_compliance_score:.2f}")
            logger.info(f"Average best practices score: {avg_best_practices_score:.2f}")
            logger.info(f"Syntax valid: {syntax_valid_pct:.1f}%")
            logger.info(f"Compliant: {compliant_pct:.1f}% (Score >= 70)")
            
            # Log top issues if any
            if all_issues:
                logger.info("\n❕ Top Issues Found:")
                # Group by description to find most common issues
                issue_counts = {}
                for issue in all_issues:
                    desc = issue.get("description", "Unknown issue")
                    issue_counts[desc] = issue_counts.get(desc, 0) + 1
                
                # Sort and show top 5
                sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
                for desc, count in sorted_issues[:5]:
                    logger.info(f"  - [{count}x] {desc}")
            
            # Log issues to MLflow for searchability
            if all_issues:
                # Tag run with issue categories for filtering
                issue_categories = set(issue.get("type", "unknown") for issue in all_issues)
                mlflow.set_tags({
                    f"has_{cat}_issues": "true" for cat in issue_categories
                })
                mlflow.set_tag("total_issues_count", len(all_issues))
                
                # Log issues as a table for querying in MLflow UI
                import pandas as pd
                issues_df = pd.DataFrame([
                    {
                        "sql_index": issue.get("sql_idx", -1),
                        "category": issue.get("type", "unknown"),
                        "severity": issue.get("severity", "warning"),
                        "description": issue.get("description", "")[:200],  # Truncate long descriptions
                    }
                    for issue in all_issues
                ])
                mlflow.log_table(issues_df, "issues_table.json")
                
                # Log top issues summary as text
                top_issues_text = "Top Issues:\n" + "\n".join(
                    f"  [{count}x] {desc}" for desc, count in sorted_issues[:10]
                )
                mlflow.log_text(top_issues_text, "top_issues_summary.txt")
            
            return {

                "model_name": model_config.name,
                "endpoint": model_config.endpoint,
                "temperature": model_config.temperature,
                "avg_compliance_score": avg_compliance_score,
                "syntax_valid_pct": syntax_valid_pct,
                "compliant_pct": compliant_pct,
                "total_sql_generated": total_results,
                "translation_duration_sec": translation_duration,
                "evaluation_duration_sec": evaluation_duration,
            }


def run_benchmark(
    artifact_type: str,
    dataset_source: Optional[str] = None,
    experiment_name: str = "sql-translation-benchmark",
    model_configs: Optional[List[ModelConfig]] = None,
    batch_size: int = 10,
    judge_endpoint: Optional[str] = None
) -> pd.DataFrame:
    """
    Convenience function to run a complete benchmark.
    
    Args:
        artifact_type: Type of artifact to benchmark
        dataset_source: Optional path to dataset file
        experiment_name: MLflow experiment name
        model_configs: Optional list of model configurations
        batch_size: Batch size for translation
        judge_endpoint: Optional LLM endpoint for judging
        
    Returns:
        DataFrame with benchmark results
    """
    runner = BenchmarkRunner(experiment_name=experiment_name)
    
    return runner.run_benchmark(
        artifact_type=artifact_type,
        dataset_source=dataset_source,
        model_configs=model_configs,
        batch_size=batch_size,
        judge_endpoint=judge_endpoint
    )


def main():
    """CLI entry point for running benchmarks."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run SQL translation model benchmarking"
    )
    parser.add_argument(
        "--artifact-type",
        type=str,
        required=True,
        help="Type of artifact to benchmark (tables, views, procedures, etc.)"
    )
    parser.add_argument(
        "--dataset-source",
        type=str,
        help="Path to dataset JSON file (optional)"
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default="sql-translation-benchmark",
        help="MLflow experiment name"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for translation"
    )
    parser.add_argument(
        "--judge-endpoint",
        type=str,
        help="Databricks LLM endpoint to use for judging (optional)"
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode with minimal data"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run benchmark
    try:
        results_df = run_benchmark(
            artifact_type=args.artifact_type,
            dataset_source=args.dataset_source,
            experiment_name=args.experiment_name,
            batch_size=args.batch_size if not args.test_mode else 2,
            judge_endpoint=args.judge_endpoint
        )
        
        print("\n" + "="*60)
        print("Benchmark Complete!")
        print("="*60)
        print(f"\nResults saved to MLflow experiment: {args.experiment_name}")
        print("\nView results in MLflow UI:")
        print("  mlflow ui")
        print("\nBenchmark Summary:")
        print(results_df.to_string())
        
    except Exception as e:
        logger.error(f"Benchmark failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
