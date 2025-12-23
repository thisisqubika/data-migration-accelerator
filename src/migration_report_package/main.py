import sys
import os
import argparse
import json
from typing import List, Dict, Any
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_builder import MigrationReportGraph

def get_output_directory() -> str:
    """
    Get the output directory path inside translation_graph folder.
    
    Returns:
        Path to output directory
    """
    output_dir = os.environ["DDL_OUTPUT_PATH"]
    return output_dir

def main():
    """Main entry point for file-based processing."""
    parser = argparse.ArgumentParser(
        description="Create a Migration Report from the translation graph outputs"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        help="directory where the translation graph outputs are stored. Default: output/ folder."
    )
    parser.add_argument(
        "--md_output",
        type=str,
        help="Custom output file path for Markdown report (optional). Default: saves to output/ folder"
    )
    
    args = parser.parse_args()
    
    print("Data Migration Accelerator - Migration Report Generator")
    print("=" * 50)
    print()

    output_dir = get_output_directory()

    input_dir = output_dir

    graph = MigrationReportGraph()

    if args.output_dir:
        input_dir = os.path.dirname(args.output_dir)
    
    md_report, json_report = graph.run(input_dir)

    print("Report done!")

    if args.md_output:
        output_dir = os.path.dirname(args.md_output)

    with open(os.path.join(output_dir, "migration_report.md"), "w", encoding="utf-8") as f:
        f.write(md_report)

    print("JSON Report: ",json_report)


if __name__ == "__main__":
    main()

