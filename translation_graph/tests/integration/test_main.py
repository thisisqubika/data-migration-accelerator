#!/usr/bin/env python3
"""
Test main.py with all example datasets.

This test validates that main.py correctly processes all artifact types:
- databases
- procedures
- schemas
- tables
- udfs
- views

Note: sequences.json is excluded as sequences support was removed.
"""

import sys
import os
import json
from pathlib import Path

# Add the translation_graph directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
translation_graph_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, translation_graph_dir)

from main import process_multiple_files, process_single_file


def get_example_data_dir():
    """Get the path to the example data directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "example_data")


def get_all_example_files():
    """Get all example JSON files (excluding sequences)."""
    example_dir = get_example_data_dir()
    files = [
        "databases.json",
        "procedures.json",
        "schemas.json",
        "tables.json",
        "udfs.json",
        "views.json"
    ]
    return [os.path.join(example_dir, f) for f in files]


def validate_file_exists(filepath: str) -> bool:
    """Check if a file exists."""
    if not os.path.exists(filepath):
        print(f"  ✗ File not found: {filepath}")
        return False
    print(f"  ✓ Found: {os.path.basename(filepath)}")
    return True


def validate_result_structure(result: dict, expected_types: list) -> bool:
    """Validate that the result has the expected structure."""
    print("\nValidating result structure...")
    
    has_metadata = "metadata" in result
    print(f"  {'✓' if has_metadata else '✗'} Result has metadata")
    
    if not has_metadata:
        return False
    
    metadata = result.get("metadata", {})
    total_results = metadata.get("total_results", 0)
    print(f"  ✓ Total results: {total_results}")
    
    errors = metadata.get("errors", [])
    if errors:
        print(f"  ⚠ Errors found: {len(errors)}")
        for error in errors[:3]:
            print(f"    - {error}")
    else:
        print(f"  ✓ No errors")
    
    found_types = []
    for artifact_type in expected_types:
        if artifact_type in result and result[artifact_type]:
            count = len(result[artifact_type])
            found_types.append(artifact_type)
            print(f"  ✓ {artifact_type}: {count} statement(s)")
        else:
            print(f"  ⚠ {artifact_type}: No results")
    
    return len(found_types) > 0


def test_main_with_all_datasets():
    """Test main.py with all example datasets."""
    print("=" * 70)
    print("Test: main.py with All Example Datasets")
    print("=" * 70)
    
    example_files = get_all_example_files()
    
    print("\n1. Checking example files...")
    valid_files = []
    for filepath in example_files:
        if validate_file_exists(filepath):
            valid_files.append(filepath)
    
    if not valid_files:
        print("\n✗ No valid files found!")
        return False
    
    print(f"\n✓ Found {len(valid_files)} valid file(s)")
    
    print("\n2. Processing files using main.py...")
    print("-" * 70)
    
    try:
        result = process_multiple_files(valid_files, batch_size=10)
        
        print("\n3. Validating results...")
        print("-" * 70)
        
        expected_types = ["databases", "procedures", "schemas", "tables", "udfs", "views"]
        is_valid = validate_result_structure(result, expected_types)
        
        if not is_valid:
            print("\n✗ Result validation failed!")
            return False
        
        print("\n4. Result Summary:")
        print("-" * 70)
        
        metadata = result.get("metadata", {})
        print(f"Total SQL statements generated: {metadata.get('total_results', 0)}")
        print(f"Evaluation results: {metadata.get('evaluation_results_count', 0)}")
        
        for artifact_type in expected_types:
            if artifact_type in result:
                statements = result[artifact_type]
                print(f"\n{artifact_type.upper()}:")
                print(f"  Generated: {len(statements)} statement(s)")
                if statements:
                    preview = statements[0][:150].replace('\n', ' ')
                    print(f"  Preview: {preview}...")
        
        print("\n" + "=" * 70)
        print("✓ Test completed successfully!")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_main_single_file():
    """Test main.py with a single file (tables.json)."""
    print("\n" + "=" * 70)
    print("Test: main.py with Single File (tables.json)")
    print("=" * 70)
    
    example_dir = get_example_data_dir()
    tables_file = os.path.join(example_dir, "tables.json")
    
    if not os.path.exists(tables_file):
        print(f"✗ File not found: {tables_file}")
        return False
    
    print(f"\n1. Processing single file: {os.path.basename(tables_file)}")
    print("-" * 70)
    
    try:
        result = process_single_file(tables_file, batch_size=10)
        
        print("\n2. Validating result...")
        print("-" * 70)
        
        if "tables" in result and result["tables"]:
            count = len(result["tables"])
            print(f"✓ Generated {count} table SQL statement(s)")
            
            if result["tables"]:
                preview = result["tables"][0][:150].replace('\n', ' ')
                print(f"  Preview: {preview}...")
            
            print("\n✓ Single file test completed successfully!")
            return True
        else:
            print("✗ No table results found!")
            return False
            
    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("Main.py Test Suite")
    print("=" * 70)
    
    results = []
    
    # Test 1: All datasets
    results.append(("All Datasets", test_main_with_all_datasets()))
    
    # Test 2: Single file
    results.append(("Single File", test_main_single_file()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())

