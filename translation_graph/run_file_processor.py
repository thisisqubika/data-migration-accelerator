#!/usr/bin/env python3
"""
File-based processor for the data migration accelerator.
Processes JSON files containing artifact definitions, determining artifact type from filename
and processing in batches.
"""

import sys
import os
import argparse
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_builder import build_translation_graph
from utils.file_processor import process_files, create_batches_from_file


def process_single_file(
    filepath: str,
    batch_size: int = 10,
    context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Process a single file and return translation results.
    
    Args:
        filepath: Path to the DDL file
        batch_size: Number of DDL statements per batch
        context: Optional context dictionary
        
    Returns:
        Translation results dictionary
    """
    print(f"Processing file: {filepath}")
    print(f"Batch size: {batch_size}")
    print("-" * 50)
    
    batches = create_batches_from_file(filepath, batch_size, context)
    
    print(f"Created {len(batches)} batch(es) from file")
    print(f"Total artifacts: {sum(len(batch.items) for batch in batches)}")
    print()
    
    graph = build_translation_graph()
    
    results = []
    for i, batch in enumerate(batches):
        print(f"Processing batch {i + 1}/{len(batches)} ({len(batch.items)} items)...")
        result = graph.run(batch)
        results.append(result)
        print(f"Batch {i + 1} completed")
        print()
    
    if len(results) == 1:
        return results[0]
    
    from nodes.aggregator import aggregate_translations
    from utils.types import TranslationResult
    
    translation_results = []
    for i, (result, batch) in enumerate(zip(results, batches)):
        translation_result = TranslationResult(
            artifact_type=batch.artifact_type,
            results=result.get(batch.artifact_type, []),
            errors=result.get("metadata", {}).get("errors", []),
            metadata={**result.get("metadata", {}), "batch_index": i}
        )
        translation_results.append(translation_result)
    
    final_result = aggregate_translations(*translation_results)
    return final_result


def process_multiple_files(
    filepaths: List[str],
    batch_size: int = 10,
    context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Process multiple files and aggregate results.
    
    Args:
        filepaths: List of file paths to process
        batch_size: Number of DDL statements per batch
        context: Optional context dictionary
        
    Returns:
        Aggregated translation results
    """
    print(f"Processing {len(filepaths)} file(s)")
    print(f"Batch size: {batch_size}")
    print("=" * 50)
    print()
    
    graph = build_translation_graph()
    all_batches = process_files(filepaths, batch_size, context)
    
    print(f"Total batches created: {len(all_batches)}")
    print()
    
    return graph.run_batches(all_batches)


def main():
    """Main entry point for file-based processing."""
    parser = argparse.ArgumentParser(
        description="Process DDL files and translate them using the translation graph"
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="One or more JSON files to process. Artifact type is determined from filename."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of artifacts per batch (default: 10)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path to save results (optional)"
    )
    
    args = parser.parse_args()
    
    print("Data Migration Accelerator - File Processor")
    print("=" * 50)
    print()
    
    if len(args.files) == 1:
        result = process_single_file(args.files[0], args.batch_size)
    else:
        result = process_multiple_files(args.files, args.batch_size)
    
    print("Translation Results:")
    print("-" * 50)
    
    for key, value in result.items():
        if key == "metadata":
            print(f"{key}:")
            for meta_key, meta_value in value.items():
                if isinstance(meta_value, dict):
                    print(f"  {meta_key}:")
                    for sub_key, sub_value in meta_value.items():
                        print(f"    {sub_key}: {sub_value}")
                else:
                    print(f"  {meta_key}: {meta_value}")
        elif isinstance(value, list):
            print(f"{key}: {len(value)} items")
            if value and len(value) <= 5:
                for item in value:
                    print(f"  - {item[:100]}..." if len(str(item)) > 100 else f"  - {item}")
        else:
            print(f"{key}: {value}")
    
    if args.output:
        import json
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\nResults saved to: {args.output}")
    
    print("\nProcessing completed successfully!")


if __name__ == "__main__":
    main()

