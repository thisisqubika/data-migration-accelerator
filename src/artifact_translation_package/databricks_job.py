#!/usr/bin/env python3
"""
Databricks Job Entry Point for Translation Graph

This module provides a Databricks-compatible interface for running the translation graph
in Databricks jobs and pipelines. It handles DBFS paths, Volumes, and Databricks-specific
configuration.
"""

import os
import json
from typing import List, Dict, Any, Optional
from pathlib import Path


from artifact_translation_package.graph_builder import build_translation_graph
from artifact_translation_package.utils.file_processor import create_batches_from_file, process_files
from artifact_translation_package.utils.types import ArtifactBatch
from artifact_translation_package.config.constants import UnityCatalogConfig, LangGraphConfig


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
    if context is None:
        context = {}
    
    context["job_type"] = "databricks"
    context["batch_size"] = batch_size
    
    print(f"Starting translation job")
    print(f"Input files: {input_files}")
    print(f"Batch size: {batch_size}")
    print(f"Output path: {output_path}")
    print(f"Output format: {output_format}")
    print("-" * 50)
    
    graph = build_translation_graph()
    
    all_batches = []
    for filepath in input_files:
        local_path = get_dbfs_path(filepath)
        print(f"Processing file: {local_path}")
        
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"File not found: {local_path}")
        
        batches = create_batches_from_file(local_path, batch_size, context)
        all_batches.extend(batches)
        print(f"  Created {len(batches)} batch(es) from {local_path}")
    
    print(f"\nTotal batches to process: {len(all_batches)}")
    print("-" * 50)
    
    result = graph.run_batches(all_batches)
    
    if output_path:
        if output_format == "json":
            output_local_path = get_dbfs_path(output_path)
            output_dir = os.path.dirname(output_local_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            with open(output_local_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, default=str)

            print(f"\nJSON results saved to: {output_local_path}")
        elif output_format == "sql":
            save_sql_files(result, output_path)

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
            print(f"JSON results saved to: {output_path}")
        elif output_format == "sql":
            save_sql_files_dbutils(result, output_path)

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
    
    json_files = []
    for file in os.listdir(volume_path):
        if file.endswith('.json'):
            if artifact_types is None:
                json_files.append(os.path.join(volume_path, file))
            else:
                file_lower = file.lower()
                if any(artifact_type.lower() in file_lower for artifact_type in artifact_types):
                    json_files.append(os.path.join(volume_path, file))
    
    if not json_files:
        print(f"No JSON files found in {volume_path}")
        return {"metadata": {"total_results": 0, "errors": []}}
    
    print(f"Found {len(json_files)} JSON file(s) to process")
    return process_translation_job(json_files, output_path, batch_size)



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

    artifact_types = ['tables', 'views', 'schemas', 'databases', 'stages', 'streams', 'pipes', 'roles', 'grants', 'tags', 'comments', 'masking_policies', 'udfs', 'procedures', 'external_locations']

    total_sql_files = 0
    total_sql_statements = 0

    for artifact_type in artifact_types:
        if artifact_type in result and result[artifact_type]:
            sql_statements = result[artifact_type]
            if sql_statements:
                # Create SQL file name
                sql_filename = f"{artifact_type}.sql"
                sql_filepath = os.path.join(output_dir, sql_filename)

                with open(sql_filepath, 'w', encoding='utf-8') as f:
                    f.write(f"-- {artifact_type.upper()} DDL - Generated by Translation Graph\n")
                    f.write(f"-- Generated: {os.path.basename(output_base_path)}\n\n")

                    for i, sql in enumerate(sql_statements, 1):
                        # Clean SQL (remove markdown code blocks if present)
                        clean_sql = sql
                        if clean_sql.startswith('```sql'):
                            clean_sql = clean_sql[6:]
                        if clean_sql.endswith('```'):
                            clean_sql = clean_sql[:-3]
                        clean_sql = clean_sql.strip()
                        # Remove any trailing semicolons to avoid double ';;' when we append one
                        while clean_sql.endswith(';'):
                            clean_sql = clean_sql[:-1].rstrip()

                        f.write(f"-- Statement {i}\n")
                        f.write(clean_sql)
                        f.write(";\n\n")

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
    import dbutils

    # Ensure output_base_path ends with a directory separator
    if not output_base_path.endswith('/'):
        output_base_path += '/'

    artifact_types = ['tables', 'views', 'schemas', 'databases', 'stages', 'streams', 'pipes', 'roles', 'grants', 'tags', 'comments', 'masking_policies', 'udfs', 'procedures', 'external_locations']

    total_sql_files = 0
    total_sql_statements = 0

    for artifact_type in artifact_types:
        if artifact_type in result and result[artifact_type]:
            sql_statements = result[artifact_type]
            if sql_statements:
                # Create SQL file path
                sql_filename = f"{artifact_type}.sql"
                sql_filepath = f"{output_base_path}{sql_filename}"

                sql_content = f"-- {artifact_type.upper()} DDL - Generated by Translation Graph\n"
                sql_content += f"-- Generated: {output_base_path}\n\n"

                for i, sql in enumerate(sql_statements, 1):
                    # Clean SQL (remove markdown code blocks if present)
                    clean_sql = sql
                    if clean_sql.startswith('```sql'):
                        clean_sql = clean_sql[6:]
                    if clean_sql.endswith('```'):
                        clean_sql = clean_sql[:-3]
                    clean_sql = clean_sql.strip()
                    # Remove any trailing semicolons to avoid double ';;' when appending one
                    while clean_sql.endswith(';'):
                        clean_sql = clean_sql[:-1].rstrip()

                    sql_content += f"-- Statement {i}\n"
                    sql_content += clean_sql
                    sql_content += ";\n\n"

                dbutils.fs.put(sql_filepath, sql_content)

                total_sql_files += 1
                total_sql_statements += len(sql_statements)
                print(f"   ✓ Saved {len(sql_statements)} {artifact_type} SQL statements to {sql_filename}")

    print(f"\nSQL files saved to: {output_base_path}")
    print(f"Total SQL files: {total_sql_files}")
    print(f"Total SQL statements: {total_sql_statements}")
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
    batch_size = LangGraphConfig.DDL_BATCH_SIZE.value
    output_format = LangGraphConfig.DDL_OUTPUT_FORMAT.value
    output_path = None  # Optional - can be None for in-memory results
    
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