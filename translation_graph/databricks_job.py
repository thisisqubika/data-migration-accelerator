#!/usr/bin/env python3
"""
Databricks Job Entry Point for Translation Graph

This module provides a Databricks-compatible interface for running the translation graph
in Databricks jobs and pipelines. It handles DBFS paths, Volumes, and Databricks-specific
configuration.
"""

import argparse
import json
import os
import sys
from typing import List, Dict, Any, Optional
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_builder import build_translation_graph
from utils.file_processor import create_batches_from_file, process_files
from utils.types import ArtifactBatch

try:
    import dbutils
    DBUTILS_AVAILABLE = True
except ImportError:
    DBUTILS_AVAILABLE = False


def get_artifact_types_list() -> List[str]:
    """Get list of all supported artifact types."""
    return [
        'tables', 'views', 'schemas', 'databases', 'sequences', 'stages', 'streams', 'pipes',
        'roles', 'grants', 'tags', 'comments', 'masking_policies', 'udfs', 'procedures',
        'external_locations', 'file_formats'
    ]


def get_dbfs_path(filepath: str) -> str:
    """
    Convert a Databricks path (dbfs:/ or /Volumes/) to a local filesystem path.
    
    Args:
        filepath: Path that may be in DBFS or Volumes format
        
    Returns:
        Local filesystem path
    """
    if filepath.startswith("dbfs:/"):
        return filepath.replace("dbfs:/", "/dbfs/")
    elif filepath.startswith("/Volumes/"):
        return filepath
    elif filepath.startswith("/dbfs/"):
        return filepath
    else:
        return filepath


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
    context = context or {}
    context.update({"job_type": "databricks", "batch_size": batch_size})

    log_job_start(input_files, batch_size, output_path, output_format)

    graph = build_translation_graph()
    all_batches = create_batches_from_files(input_files, batch_size, context)

    print(f"\nTotal batches to process: {len(all_batches)}")
    print("-" * 50)

    result = graph.run_batches(all_batches)

    if output_path:
        save_result(result, output_path, output_format)

    return result


def log_job_start(input_files: List[str], batch_size: int, output_path: Optional[str], output_format: str):
    """Log job start information."""
    print("Starting translation job")
    print(f"Input files: {input_files}")
    print(f"Batch size: {batch_size}")
    print(f"Output path: {output_path}")
    print(f"Output format: {output_format}")
    print("-" * 50)


def create_batches_from_files(input_files: List[str], batch_size: int, context: Dict[str, Any]) -> List[ArtifactBatch]:
    """Create batches from multiple input files."""
    all_batches = []
    for filepath in input_files:
        local_path = get_dbfs_path(filepath)
        print(f"Processing file: {local_path}")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"File not found: {local_path}")

        batches = create_batches_from_file(local_path, batch_size, context)
        all_batches.extend(batches)
        print(f"  Created {len(batches)} batch(es) from {local_path}")

    return all_batches


def save_result(result: Dict[str, Any], output_path: str, output_format: str):
    """Save translation result to specified location and format."""
    if output_format == "json":
        save_json_result(result, output_path)
    elif output_format == "sql":
        save_sql_files(result, output_path)


def save_json_result(result: Dict[str, Any], output_path: str):
    """Save result as JSON file."""
    output_local_path = get_dbfs_path(output_path)
    output_dir = os.path.dirname(output_local_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(output_local_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\nJSON results saved to: {output_local_path}")


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
    if not DBUTILS_AVAILABLE:
        raise ImportError(
            "dbutils not available. Use process_translation_job() instead or run in Databricks environment."
        )

    context = {
        "job_type": "databricks",
        "batch_size": batch_size,
        "using_dbutils": True
    }

    graph = build_translation_graph()
    all_batches = []

    for filepath in input_files:
        print(f"Reading file: {filepath}")
        batches = create_batches_from_file_with_dbutils(filepath, batch_size, context)
        all_batches.extend(batches)

    result = graph.run_batches(all_batches)

    if output_path:
        save_result_with_dbutils(result, output_path, output_format)

    return result


def create_batches_from_file_with_dbutils(filepath: str, batch_size: int, context: Dict[str, Any]) -> List[ArtifactBatch]:
    """Create batches from file using dbutils if needed."""
    if filepath.startswith("dbfs:/"):
        content = dbutils.fs.head(filepath)
        local_temp_path = f"/tmp/{os.path.basename(filepath)}"

        with open(local_temp_path, 'w', encoding='utf-8') as f:
            f.write(content)

        batches = create_batches_from_file(local_temp_path, batch_size, context)
        os.remove(local_temp_path)
        return batches
    else:
        return create_batches_from_file(filepath, batch_size, context)


def save_result_with_dbutils(result: Dict[str, Any], output_path: str, output_format: str):
    """Save result using dbutils."""
    if output_format == "json":
        output_json = json.dumps(result, indent=2, default=str)
        dbutils.fs.put(output_path, output_json)
        print(f"JSON results saved to: {output_path}")
    elif output_format == "sql":
        save_sql_files_dbutils(result, output_path)


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
    validate_volume_path(volume_path)
    json_files = find_json_files_in_volume(volume_path, artifact_types)

    if not json_files:
        print(f"No JSON files found in {volume_path}")
        return {"metadata": {"total_results": 0, "errors": []}}

    print(f"Found {len(json_files)} JSON file(s) to process")
    return process_translation_job(json_files, output_path, batch_size, output_format=output_format)


def validate_volume_path(volume_path: str):
    """Validate that the volume path is correct."""
    if not volume_path.startswith("/Volumes/"):
        raise ValueError(f"Volume path must start with /Volumes/, got: {volume_path}")

    if not os.path.exists(volume_path):
        raise FileNotFoundError(f"Volume path not found: {volume_path}")


def find_json_files_in_volume(volume_path: str, artifact_types: Optional[List[str]] = None) -> List[str]:
    """Find JSON files in volume directory, optionally filtering by artifact types."""
    json_files = []
    for file in os.listdir(volume_path):
        if file.endswith('.json'):
            if artifact_types is None or matches_artifact_type(file, artifact_types):
                json_files.append(os.path.join(volume_path, file))
    return json_files


def matches_artifact_type(filename: str, artifact_types: List[str]) -> bool:
    """Check if filename matches any of the specified artifact types."""
    file_lower = filename.lower()
    return any(artifact_type.lower() in file_lower for artifact_type in artifact_types)



def clean_sql_statement(sql: str) -> str:
    """
    Clean SQL statement by removing markdown code blocks and normalizing.

    Args:
        sql: Raw SQL statement

    Returns:
        Cleaned SQL statement
    """
    clean_sql = sql
    if clean_sql.startswith('```sql'):
        clean_sql = clean_sql[6:]
    if clean_sql.endswith('```'):
        clean_sql = clean_sql[:-3]
    return clean_sql.strip()


def format_sql_content(artifact_type: str, sql_statements: List[str], output_base_path: str) -> str:
    """
    Format SQL statements into file content with headers and comments.

    Args:
        artifact_type: Type of artifact (tables, views, etc.)
        sql_statements: List of SQL statements
        output_base_path: Base path for header comment

    Returns:
        Formatted SQL file content
    """
    content = f"-- {artifact_type.upper()} DDL - Generated by Translation Graph\n"
    content += f"-- Generated: {os.path.basename(output_base_path)}\n\n"

    for i, sql in enumerate(sql_statements, 1):
        clean_sql = clean_sql_statement(sql)
        content += f"-- Statement {i}\n"
        content += clean_sql
        if not clean_sql.rstrip().endswith(';'):
            content += ";"
        content += "\n\n"

    return content


def save_sql_files(result: Dict[str, Any], output_base_path: str):
    """
    Save SQL results as separate SQL files for each artifact type.

    Args:
        result: Translation results dictionary
        output_base_path: Base path where SQL files will be saved
    """
    output_local_path = get_dbfs_path(output_base_path)
    output_dir = os.path.dirname(output_local_path) if os.path.isfile(output_local_path) else output_local_path

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    artifact_types = get_artifact_types_list()
    total_sql_files = 0
    total_sql_statements = 0

    for artifact_type in artifact_types:
        if artifact_type in result and result[artifact_type]:
            sql_statements = result[artifact_type]
            if sql_statements:
                sql_filename = f"{artifact_type}.sql"
                sql_filepath = os.path.join(output_dir, sql_filename)

                content = format_sql_content(artifact_type, sql_statements, output_base_path)
                with open(sql_filepath, 'w', encoding='utf-8') as f:
                    f.write(content)

                total_sql_files += 1
                total_sql_statements += len(sql_statements)
                print(f"   ✓ Saved {len(sql_statements)} {artifact_type} SQL statements to {sql_filename}")

    print(f"\nSQL files saved to: {output_dir}")
    print(f"Total SQL files: {total_sql_files}")
    print(f"Total SQL statements: {total_sql_statements}")


def save_sql_files_dbutils(result: Dict[str, Any], output_base_path: str):
    """
    Save SQL results as separate SQL files using dbutils.

    Args:
        result: Translation results dictionary
        output_base_path: Base path where SQL files will be saved (dbfs:/ paths)
    """
    if not output_base_path.endswith('/'):
        output_base_path += '/'

    artifact_types = get_artifact_types_list()

    total_sql_files = 0
    total_sql_statements = 0

    for artifact_type in artifact_types:
        if artifact_type in result and result[artifact_type]:
            sql_statements = result[artifact_type]
            if sql_statements:
                sql_filename = f"{artifact_type}.sql"
                sql_filepath = f"{output_base_path}{sql_filename}"

                content = format_sql_content(artifact_type, sql_statements, output_base_path)
                dbutils.fs.put(sql_filepath, content)

                total_sql_files += 1
                total_sql_statements += len(sql_statements)
                print(f"   ✓ Saved {len(sql_statements)} {artifact_type} SQL statements to {sql_filename}")

    print(f"\nSQL files saved to: {output_base_path}")
    print(f"Total SQL files: {total_sql_files}")
    print(f"Total SQL statements: {total_sql_statements}")

def create_argument_parser():
    """Create and configure the argument parser."""
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
    return parser


def print_job_summary(result: Dict[str, Any]):
    """Print job completion summary."""
    print("\n" + "=" * 50)
    print("Translation Job Completed")
    print("=" * 50)
    metadata = result.get('metadata', {})
    print(f"Total results: {metadata.get('total_results', 0)}")
    print(f"Errors: {len(metadata.get('errors', []))}")


if __name__ == "__main__":
    parser = create_argument_parser()
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

    print_job_summary(result)


