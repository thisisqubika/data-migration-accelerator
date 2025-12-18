#!/usr/bin/env python3
"""
Full Workflow Integration Test

This comprehensive test demonstrates the complete Translation Graph workflow:
1. Loads example JSON files containing Snowflake object definitions
2. Processes them through the translation graph with real LLM calls
3. Validates the generated Databricks SQL DDL results
4. Saves complete results for inspection

Test scenarios:
- Table translation with real LLM-generated DDL
- Multiple file processing (tables, views, schemas)
- Result persistence and validation
"""

import sys
import os
import json
from pathlib import Path

from artifact_translation_package.graph_builder import build_translation_graph
from artifact_translation_package.utils.file_processor import create_batches_from_file, process_files


def get_example_data_dir():
    """Get the path to the example data directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "example_data")


def test_translate_tables():
    """Test translating tables from example JSON file."""
    print("=" * 70)
    print("Integration Test: Translating Tables")
    print("=" * 70)
    
    example_dir = get_example_data_dir()
    tables_file = os.path.join(example_dir, "tables.json")
    
    if not os.path.exists(tables_file):
        print(f"ERROR: Example file not found: {tables_file}")
        return False
    
    print(f"\n1. Loading example file: {tables_file}")
    
    try:
        batches = create_batches_from_file(tables_file, batch_size=10)
        print(f"   ✓ Created {len(batches)} batch(es)")
        print(f"   ✓ Total artifacts: {sum(len(batch.items) for batch in batches)}")
        
        print(f"\n2. Building translation graph...")
        graph = build_translation_graph()
        print("   ✓ Graph built successfully")
        
        print(f"\n3. Processing batches through translation graph...")
        result = graph.run_batches(batches)
        
        print(f"\n4. Results:")
        metadata = result.get('metadata', {})
        print(f"   ✓ Total SQL statements: {metadata.get('total_results', 0)}")
        print(f"   ✓ Errors: {len(metadata.get('errors', []))}")

        if result.get('tables'):
            print(f"   ✓ Table SQL statements: {len(result['tables'])}")
            for i, table_sql in enumerate(result['tables'][:2], 1):
                # Clean up the SQL for display (remove code blocks if present)
                if table_sql.startswith('```sql'):
                    table_sql = table_sql[6:]
                if table_sql.endswith('```'):
                    table_sql = table_sql[:-3]
                preview = table_sql.strip()[:100] + "..." if len(table_sql.strip()) > 100 else table_sql.strip()
                print(f"     Table {i}: {preview}")
        
        if result.get('metadata', {}).get('errors'):
            print(f"\n   ⚠ Errors encountered:")
            for error in result['metadata']['errors'][:3]:
                print(f"     - {error}")
        
        print(f"\n✓ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_translate_multiple_files():
    """Test translating multiple files (tables, views, schemas)."""
    print("\n" + "=" * 70)
    print("Integration Test: Translating Multiple Files")
    print("=" * 70)

    example_dir = get_example_data_dir()
    files = [
        os.path.join(example_dir, "tables.json"),
        os.path.join(example_dir, "views.json"),
        os.path.join(example_dir, "schemas.json"),
        os.path.join(example_dir, "databases.json"),
        os.path.join(example_dir, "procedures.json"),
        os.path.join(example_dir, "udfs.json")
    ]

    existing_files = [f for f in files if os.path.exists(f)]

    if not existing_files:
        print(f"ERROR: No example files found in {example_dir}")
        return False

    print(f"\n1. Processing {len(existing_files)} file(s)...")
    for f in existing_files:
        print(f"   - {os.path.basename(f)}")

    try:
        batches = process_files(existing_files, batch_size=5)
        print(f"\n2. Created {len(batches)} batch(es) from all files")

        print(f"\n3. Building translation graph...")
        graph = build_translation_graph()

        print(f"\n4. Processing all batches...")
        result = graph.run_batches(batches)

        print(f"\n5. Results Summary:")
        metadata = result.get('metadata', {})
        print(f"   ✓ Total results: {metadata.get('total_results', 0)}")
        print(f"   ✓ Errors: {len(metadata.get('errors', []))}")

        print(f"\n6. Generated SQL by artifact type:")
        total_sql_statements = 0
        for artifact_type in ['tables', 'views', 'schemas', 'databases',  'stages', 'streams', 'pipes', 'roles', 'grants', 'tags', 'comments', 'masking_policies', 'udfs', 'procedures', 'external_locations', 'file_formats']:
            if artifact_type in result and result[artifact_type]:
                sql_count = len(result[artifact_type])
                total_sql_statements += sql_count
                print(f"   ✓ {artifact_type}: {sql_count} SQL statement(s)")

                # Show first SQL statement as preview for each type
                if sql_count > 0:
                    first_sql = result[artifact_type][0]
                    # Clean up the SQL for display (remove code blocks if present)
                    if first_sql.startswith('```sql'):
                        first_sql = first_sql[6:]
                    if first_sql.endswith('```'):
                        first_sql = first_sql[:-3]
                    preview = first_sql.strip()[:80] + "..." if len(first_sql.strip()) > 80 else first_sql.strip()
                    print(f"     Preview: {preview}")

        print(f"\n7. Total SQL statements generated: {total_sql_statements}")

        if metadata.get('errors'):
            print(f"\n8. Errors encountered:")
            for error in metadata['errors'][:3]:  # Show first 3 errors
                print(f"   - {error}")

        print(f"\n✓ Test completed successfully!")
        return True

    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_save_sql_files():
    """Test saving results as separate SQL files for each artifact type."""
    print("\n" + "=" * 70)
    print("Integration Test: Saving SQL Files")
    print("=" * 70)

    example_dir = get_example_data_dir()
    files = [
        os.path.join(example_dir, "tables.json"),
        os.path.join(example_dir, "views.json"),
        os.path.join(example_dir, "schemas.json"),
        os.path.join(example_dir, "databases.json"),
        os.path.join(example_dir, "procedures.json"),
        os.path.join(example_dir, "udfs.json")
    ]
    output_dir = os.path.join(example_dir, "output", "sql_files")

    existing_files = [f for f in files if os.path.exists(f)]
    if not existing_files:
        print(f"ERROR: No example files found in {example_dir}")
        return False

    try:
        print(f"\n1. Processing {len(files)} file(s)...")
        for file_path in files:
            print(f"   - {os.path.basename(file_path)}")

        batches = []
        for file_path in files:
            file_batches = create_batches_from_file(file_path, batch_size=10)
            batches.extend(file_batches)

        print(f"\n2. Running translation...")
        graph = build_translation_graph()
        result = graph.run_batches(batches)

        print(f"\n3. Saving SQL files to: {output_dir}")

        # Import the save function from the artifact package
        from artifact_translation_package.databricks_job import save_sql_files

        save_sql_files(result, output_dir)

        # Verify SQL files were created
        sql_files_created = []
        if os.path.exists(output_dir):
            for file in os.listdir(output_dir):
                if file.endswith('.sql'):
                    sql_files_created.append(file)

        print(f"\n4. Verification:")
        print(f"   ✓ SQL files created: {len(sql_files_created)}")
        total_sql_statements = 0

        for sql_file in sorted(sql_files_created):
            file_path = os.path.join(output_dir, sql_file)
            file_size = os.path.getsize(file_path)

            # Count SQL statements (lines starting with non-comment content)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                sql_statements = [line for line in lines if line.strip() and not line.strip().startswith('--')]
                sql_count = len(sql_statements)

            total_sql_statements += sql_count
            print(f"     - {sql_file}: {file_size} bytes ({sql_count} SQL statements)")

            # Show first few lines as preview
            preview_lines = []
            for line in lines[:3]:
                if line.strip() and not line.strip().startswith('--'):
                    preview_lines.append(line.strip())
                    if len(preview_lines) >= 2:
                        break

            if preview_lines:
                preview = ' '.join(preview_lines)
                if len(preview) > 80:
                    preview = preview[:80] + "..."
                print(f"       Preview: {preview}")

        print(f"\n   ✓ Total SQL statements saved: {total_sql_statements}")
        if sql_files_created:
            types = [f.split('.')[0] for f in sql_files_created]
            print(f"   ✓ Types saved: {', '.join(sorted(types))}")

        print(f"\n✓ Test completed successfully!")
        return True

    except Exception as e:
        print(f"\n✗ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all integration tests."""
    print("\n" + "=" * 70)
    print("Translation Graph Integration Tests")
    print("=" * 70)
    print("\nRunning integration tests with example data files...")
    
    results = []
    
    # Test 1: Translate tables
    results.append(("Translate Tables", test_translate_tables()))
    
    # Test 2: Translate multiple files
    results.append(("Translate Multiple Files", test_translate_multiple_files()))
    
    # Test 3: Save SQL files
    results.append(("Save SQL Files", test_save_sql_files()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


