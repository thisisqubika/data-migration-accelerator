#!/usr/bin/env python3
"""Databricks job entrypoint and local runner helpers.

Provides utilities to resolve DBFS/Volume paths and to run the translation
graph both on Databricks and locally while preserving the same output semantics.
"""

import os
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from artifact_translation_package.utils.output_utils import make_timestamped_output_path, is_databricks_env
from artifact_translation_package.utils.sql_file_writer import save_sql_files


from artifact_translation_package.graph_builder import build_translation_graph
from artifact_translation_package.utils.file_processor import create_batches_from_file, process_files
from artifact_translation_package.utils.types import ArtifactBatch
from artifact_translation_package.config.constants import UnityCatalogConfig, LangGraphConfig
from artifact_translation_package.utils.logger import get_logger
from artifact_translation_package.utils.output_utils import utc_timestamp


def get_dbfs_path(filepath: str) -> str:
    """
    Convert a Databricks path (dbfs:/ or /Volumes/) to a local filesystem path.
    
    Args:
        filepath: Path that may be in DBFS or Volumes format
        
    Returns:
        Local filesystem path
    """
    # Map dbfs:/ paths to an appropriate local path depending on runtime.
    if filepath.startswith("dbfs:/"):
        # On Databricks, use the /dbfs/ mount for local file operations.
        if is_databricks_env():
            return filepath.replace("dbfs:/", "/dbfs/")
        # When running locally, allow mapping dbfs:/ to a local directory
        # configured via LOCAL_DBFS_MOUNT (default: ./ddl_output)
        local_base = os.environ.get("LOCAL_DBFS_MOUNT", "./ddl_output")
        # Build local path: dbfs:/a/b/c -> <local_base>/a/b/c
        path_part = filepath[len("dbfs:/"):]
        return os.path.join(local_base.rstrip(os.path.sep), path_part.lstrip("/"))
    elif filepath.startswith("/Volumes/"):
        return filepath
    elif filepath.startswith("/dbfs/"):
        return filepath
    else:
        return filepath


def _create_results_dir_from_output(output_path: str, output_format: str) -> Optional[str]:
    """Create and return a filesystem path to a per-run results directory.

    Returns a local filesystem path even when `output_path` is a `dbfs:/` path
    (it maps to `LOCAL_DBFS_MOUNT` when running locally).
    """
    if not output_path:
        return None

    try:
        if output_format == "json":
            ts_path = make_timestamped_output_path(output_path, "json")
            return os.path.dirname(get_dbfs_path(ts_path))
        else:
            import re
            timestamp_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$")
            base_local = get_dbfs_path(output_path)
            
            # Check if the path already ends with a timestamp
            segments = [s for s in base_local.split(os.path.sep) if s]
            last_seg = segments[-1] if segments else ''
            
            if timestamp_pattern.match(last_seg):
                # Path already has a timestamp, use it as-is
                return base_local
            else:
                # Add a new timestamp
                ts = utc_timestamp()
                return os.path.join(base_local, ts)
    except Exception:
        return None


def _persist_evaluation_and_summary(result: Dict[str, Any], output_dir: str, logger) -> None:
    """Persist evaluation, translation results and observability summary into `output_dir`."""
    if not output_dir:
        return

    # Persist evaluation/validation results
    eval_results = result.get("evaluation_results")
    if eval_results:
        eval_dir = os.path.join(output_dir, "evaluations")
        os.makedirs(eval_dir, exist_ok=True)
        eval_path = os.path.join(eval_dir, "evaluation_results.json")
        with open(eval_path, 'w', encoding='utf-8') as f:
            json.dump(eval_results, f, indent=2, default=str)
        logger.info("Evaluation/validation results saved", {"path": eval_path, "count": len(eval_results)})

    # Persist main translation results
    translation_path = os.path.join(output_dir, "translation_results.json")
    with open(translation_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("Translation results saved", {"path": translation_path})

    # Persist summary/observability if present
    summary = result.get("observability")
    if summary:
        summary_path = os.path.join(output_dir, "results_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info("Results summary saved", {"path": summary_path})


def _setup_job_context(
    context: Optional[Dict[str, Any]],
    batch_size: int
) -> Dict[str, Any]:
    """
    Setup job context with job type and batch size.
    
    Args:
        context: Optional context dictionary
        batch_size: Number of artifacts per batch
        
    Returns:
        Updated context dictionary
    """
    if context is None:
        context = {}
    
    context["job_type"] = "databricks"
    context["batch_size"] = batch_size
    return context


def _setup_output_directory(
    output_path: str,
    output_format: str,
    context: Dict[str, Any]
) -> Optional[str]:
    """
    Setup output directory and inject into context.
    
    Args:
        output_path: Output path for results
        output_format: Output format (json/sql)
        context: Context dictionary to update
        
    Returns:
        Results directory path or None
    """
    results_dir = _create_results_dir_from_output(output_path, output_format)
    if results_dir and not os.path.exists(results_dir):
        os.makedirs(results_dir, exist_ok=True)
    if results_dir:
        context["results_dir"] = results_dir
        try:
            os.environ["DDL_OUTPUT_DIR"] = results_dir
        except Exception:
            pass
    return results_dir


def _process_all_batches(
    input_files: List[str],
    batch_size: int,
    context: Dict[str, Any]
) -> List:
    """
    Process all input files and create batches.
    
    Args:
        input_files: List of JSON file paths
        batch_size: Number of artifacts per batch
        context: Context dictionary
        
    Returns:
        List of all batches
    """
    all_batches = []
    for filepath in input_files:
        local_path = get_dbfs_path(filepath)
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"File not found: {local_path}")
        batches = create_batches_from_file(local_path, batch_size, context)
        all_batches.extend(batches)
    return all_batches


def _save_results_by_format(
    result: Dict[str, Any],
    output_path: str,
    output_format: str,
    context: Dict[str, Any],
    logger
) -> Optional[str]:
    """
    Save results based on output format.
    
    Args:
        result: Translation results dictionary
        output_path: Output path for results
        output_format: Output format (json/sql)
        context: Context dictionary
        logger: Logger instance
        
    Returns:
        Output directory path or None
    """
    if output_format == "json":
        return _save_json_results(result, output_path, logger)
    elif output_format == "sql":
        return _save_sql_results(result, output_path, context, logger)
    return None


def _save_json_results(
    result: Dict[str, Any],
    output_path: str,
    logger
) -> str:
    """
    Save results as JSON file.
    
    Args:
        result: Translation results dictionary
        output_path: Output path for results
        logger: Logger instance
        
    Returns:
        Output directory path
    """
    output_local_path = get_dbfs_path(output_path)
    output_dir = os.path.dirname(output_local_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    # Save main results
    with open(output_local_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("JSON results saved", {"path": output_local_path, "total_results": result.get("metadata", {}).get("total_results", 0)})
    return output_dir


def _save_sql_results(
    result: Dict[str, Any],
    output_path: str,
    context: Dict[str, Any],
    logger
) -> str:
    """
    Save results as SQL files.
    
    Args:
        result: Translation results dictionary
        output_path: Output path for results
        context: Context dictionary
        logger: Logger instance
        
    Returns:
        Output directory path
    """
    results_dir = context.get("results_dir")
    if results_dir:
        save_sql_files(result, results_dir, use_dbutils=False, logger=logger)
        logger.info("SQL files saved", {"path": results_dir})
        return results_dir
    else:
        save_sql_files(result, output_path, use_dbutils=False, logger=logger)
        logger.info("SQL files saved", {"path": output_path})
        output_local_path = get_dbfs_path(output_path)
        ts = None
        if os.path.exists(output_local_path) and os.path.isdir(output_local_path):
            subdirs = [d for d in os.listdir(output_local_path) if os.path.isdir(os.path.join(output_local_path, d))]
            if subdirs:
                ts = max(subdirs)
        return os.path.join(output_local_path, ts) if ts else output_local_path


def process_translation_job(
    input_files: List[str],
    output_path: Optional[str] = None,
    batch_size: int = 10,
    context: Optional[Dict[str, Any]] = None,
    output_format: str = "json"
) -> Dict[str, Any]:
    """
    Main entry point for Databricks job processing.
    
    Args:
        input_files: List of JSON file paths (supports dbfs:/, /Volumes/, or local paths)
        output_path: Optional output path for results (default: writes to job output)
        batch_size: Number of artifacts per batch
        context: Optional context dictionary
        
    Returns:
        Translation results dictionary
    """
    context = _setup_job_context(context, batch_size)
    
    if output_path:
        _setup_output_directory(output_path, output_format, context)

    logger = get_logger("databricks_job")
    graph = build_translation_graph()
    all_batches = _process_all_batches(input_files, batch_size, context)
    result = graph.run_batches(all_batches)
    
    if output_path:
        output_dir = _save_results_by_format(result, output_path, output_format, context, logger)
        if output_dir:
            _persist_evaluation_and_summary(result, output_dir, logger)
    
    return result


def process_from_dbutils(
    input_files: List[str],
    output_path: Optional[str] = None,
    batch_size: int = 10,
    output_format: str = "json"
) -> Dict[str, Any]:
    """
    Process translation job using dbutils for file access.
    
    This is useful when files are in DBFS and you want to use dbutils.fs operations.
    
    Args:
        input_files: List of file paths (dbfs:/ paths)
        output_path: Optional output path
        batch_size: Number of artifacts per batch
        
    Returns:
        Translation results dictionary
    """
    try:
        import dbutils
    except ImportError:
        raise ImportError("dbutils not available. Use process_translation_job() instead or run in Databricks environment.")
    context = {
        "job_type": "databricks",
        "batch_size": batch_size,
        "using_dbutils": True
    }
    graph = build_translation_graph()
    all_batches = []
    for filepath in input_files:
        if filepath.startswith("dbfs:/"):
            content = dbutils.fs.head(filepath)
            local_temp_path = f"/tmp/{os.path.basename(filepath)}"
            with open(local_temp_path, 'w', encoding='utf-8') as f:
                f.write(content)
            batches = create_batches_from_file(local_temp_path, batch_size, context)
            all_batches.extend(batches)
            os.remove(local_temp_path)
        else:
            batches = create_batches_from_file(filepath, batch_size, context)
            all_batches.extend(batches)
    result = graph.run_batches(all_batches)
    if output_path:
        if output_format == "json":
            output_json = json.dumps(result, indent=2, default=str)
            dbutils.fs.put(output_path, output_json)
            logger = get_logger("databricks_job")
            logger.info("JSON results saved to dbfs", {"path": output_path, "total_results": result.get("metadata", {}).get("total_results", 0)})
        elif output_format == "sql":
            save_sql_files(result, output_path, use_dbutils=True, logger=logger)
    return result


def process_from_volume(
    volume_path: str,
    artifact_types: Optional[List[str]] = None,
    output_path: Optional[str] = None,
    batch_size: int = 10,
    output_format: str = "json"
) -> Dict[str, Any]:
    """
    Process all JSON files from a Volume directory.
    
    Args:
        volume_path: Path to Volume directory (e.g., /Volumes/catalog/schema/volume_name/)
        artifact_types: Optional list of artifact types to process (if None, processes all)
        output_path: Optional output path for results
        batch_size: Number of artifacts per batch
        
    Returns:
        Translation results dictionary
    """
    if not volume_path.startswith("/Volumes/"):
        raise ValueError(f"Volume path must start with /Volumes/, got: {volume_path}")
    if not os.path.exists(volume_path):
        raise FileNotFoundError(f"Volume path not found: {volume_path}")
    json_files = [os.path.join(volume_path, file)
                  for file in os.listdir(volume_path)
                  if file.endswith('.json') and (
                      artifact_types is None or any(artifact_type.lower() in file.lower() for artifact_type in artifact_types)
                  )]
    if not json_files:
        return {"metadata": {"total_results": 0, "errors": []}}
    return process_translation_job(json_files, output_path, batch_size, output_format=output_format)





def main():
    """Main entry point for Databricks translation jobs."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Databricks translation job entry point"
    )
    parser.add_argument(
        "--input-files",
        nargs="+",
        required=True,
        help="Input JSON files (supports dbfs:/, /Volumes/, or local paths)"
    )
    parser.add_argument(
        "--output-path",
        type=str,
        help="Output path for results (JSON or directory for SQL files)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of artifacts per batch (default: 10)"
    )
    parser.add_argument(
        "--volume-path",
        type=str,
        help="Process all JSON files from a Volume directory"
    )
    parser.add_argument(
        "--artifact-types",
        nargs="+",
        help="Filter artifact types when using --volume-path"
    )
    parser.add_argument(
        "--output-format",
        choices=["json", "sql"],
        default="json",
        help="Output format: json or sql files (default: json)"
    )

    args = parser.parse_args()
    
    if args.volume_path:
        result = process_from_volume(
            args.volume_path,
            args.artifact_types,
            args.output_path,
            args.batch_size,
            args.output_format
        )
    else:
        result = process_translation_job(
            args.input_files,
            args.output_path,
            args.batch_size,
            output_format=args.output_format
        )
    
    print("\n" + "=" * 50)
    print("Translation Job Completed")
    print("=" * 50)
    print(f"Total results: {result.get('metadata', {}).get('total_results', 0)}")
    print(f"Errors: {len(result.get('metadata', {}).get('errors', []))}")

def databricks_entrypoint():
    """
    Simplified entry point for Databricks jobs using configuration constants.
    No CLI arguments required - uses constants directly.
    """    
    # Use constants directly - no environment variables or complex logic
    volume_path = (
        f"/Volumes/"
        f"{UnityCatalogConfig.CATALOG.value}/"
        f"{UnityCatalogConfig.SCHEMA.value}/"
        f"{UnityCatalogConfig.RAW_VOLUME.value}/"
    )
    # Respect env vars with fallbacks to constants so cluster/job-level env can override defaults
    batch_size = int(os.environ.get("DDL_BATCH_SIZE") or LangGraphConfig.DDL_BATCH_SIZE.value)
    output_format = (os.environ.get("DDL_OUTPUT_FORMAT") or LangGraphConfig.DDL_OUTPUT_FORMAT.value).lower()
    # Default output_path comes from the LangGraphConfig constant.
    # Allow an environment variable `DDL_OUTPUT_PATH` to override this (useful in Databricks jobs).
    output_path = os.environ.get("DDL_OUTPUT_PATH") or LangGraphConfig.DDL_OUTPUT_DIR.value
    # Treat empty string as no output (in-memory)
    if output_path == "":
        output_path = None
    output_path = make_timestamped_output_path(output_path, output_format)
    print("=" * 60)
    print("Databricks Translation Job Starting")
    print("=" * 60)
    print(f"Volume Path:   {volume_path}")
    print(f"Batch Size:    {batch_size}")
    print(f"Output Format: {output_format}")
    print(f"Output Path:   {output_path or 'In-memory (no file output)'}")
    print("-" * 60)
    result = process_from_volume(
        volume_path=volume_path,
        batch_size=batch_size,
        output_format=output_format,
        output_path=output_path
    )
    print("\n" + "=" * 60)
    print("Translation Job Completed")
    print("=" * 60)
    print(f"Total Results: {result.get('metadata', {}).get('total_results', 0)}")
    print(f"Errors:        {len(result.get('metadata', {}).get('errors', []))}")
    print("=" * 60)
    return result

if __name__ == "__main__":
    main()