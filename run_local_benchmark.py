#!/usr/bin/env python3
"""
Run evaluation benchmark locally with Databricks connection.

Usage:
    python3 run_local_benchmark.py
    python3 run_local_benchmark.py --artifact-type tables --batch-size 5
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"✅ Loaded .env from: {env_path}")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import mlflow
from databricks_langchain import ChatDatabricks

from artifact_translation_package.evaluation import run_benchmark, ModelConfig
from artifact_translation_package.evaluation.model_benchmark import create_default_model_configs


def get_experiment_name(custom_name: str = None) -> str:
    """Get experiment name, auto-detecting user if not provided."""
    if custom_name:
        return custom_name
    try:
        from databricks.sdk import WorkspaceClient
        username = WorkspaceClient().current_user.me().user_name
        return f"/Users/{username}/sql-translation-benchmark"
    except Exception:
        return "/Shared/sql-translation-benchmark"


def setup_mlflow(experiment_name: str) -> str:
    """Configure MLflow with Databricks tracking."""
    mlflow.set_tracking_uri("databricks")
    
    try:
        mlflow.set_experiment(experiment_name)
    except Exception:
        mlflow.create_experiment(experiment_name)
        mlflow.set_experiment(experiment_name)
    
    print(f"✅ MLflow experiment: {experiment_name}")
    return experiment_name


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="SQL translation benchmark")
    parser.add_argument("--artifact-type", default="tables", help="Artifact type")
    parser.add_argument("--dataset-source", help="Dataset JSON path")
    parser.add_argument("--experiment-name", help="MLflow experiment name")
    parser.add_argument("--batch-size", type=int, default=5, help="Batch size")
    parser.add_argument("--judge-endpoint", default="databricks-llama-4-maverick")
    parser.add_argument("--models", nargs="+", help="Model endpoints to test")
    return parser.parse_args()


def main():
    """Run benchmark."""
    args = parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    print("\n" + "="*70)
    print("🚀 SQL Translation Benchmark")
    print("="*70)
    
    experiment_name = setup_mlflow(get_experiment_name(args.experiment_name))
    
    # Configure models
    if args.models:
        model_configs = [
            ModelConfig(name=ep, endpoint=ep, temperature=0.1, max_tokens=4000)
            for ep in args.models
        ]
    else:
        model_configs = create_default_model_configs()
    
    print(f"Models: {[c.name for c in model_configs]}")
    print(f"Artifact: {args.artifact_type}, Batch: {args.batch_size}")
    
    try:
        results_df = run_benchmark(
            artifact_type=args.artifact_type,
            dataset_source=args.dataset_source,
            experiment_name=experiment_name,
            model_configs=model_configs,
            batch_size=args.batch_size,
            judge_endpoint=args.judge_endpoint
        )
        
        print("\n" + "="*70)
        print("✅ Benchmark Complete!")
        print("="*70)
        print(results_df.to_string())
        print(f"\n🔗 View results: {experiment_name}")
        
    except Exception as e:
        print(f"\n❌ Failed: {e}")
        logging.error("Benchmark error", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
