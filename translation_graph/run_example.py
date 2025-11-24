#!/usr/bin/env python3
"""
Example runner for the data migration accelerator.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_builder import build_translation_graph
from utils.types import ArtifactBatch


def main():
    """Run the example translation workflow."""
    print("Data Migration Accelerator - Example Run")
    print("=" * 50)

    # Create a fake ArtifactBatch
    batch = ArtifactBatch(
        artifact_type="tables",
        items=["CREATE TABLE example (id INT, name VARCHAR(255))"],
        context={"source_db": "snowflake", "target_db": "postgres"}
    )

    print(f"Processing batch: {batch.artifact_type}")
    print(f"Items count: {len(batch.items)}")
    print(f"Context: {batch.context}")
    print()

    # Build the translation graph
    graph = build_translation_graph()

    # Run the graph
    print("Running translation graph...")
    result = graph.run(batch)

    # Print the result
    print("Translation Results:")
    print("-" * 20)
    for key, value in result.items():
        if key == "metadata":
            print(f"{key}:")
            for meta_key, meta_value in value.items():
                print(f"  {meta_key}: {meta_value}")
        else:
            print(f"{key}: {value}")

    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
