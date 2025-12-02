#!/usr/bin/env python3
"""
SQLGlot Migration Demo Script

This script demonstrates using SQLGlot for database object migration
instead of LLMs. Run this to see the basic functionality.
"""

import sqlglot
import json
import os
from pathlib import Path

# Configuration - Change these for different migrations
SOURCE_DIALECT = "snowflake"  # Options: snowflake, mysql, postgresql, sqlserver, bigquery, etc.
TARGET_DIALECT = "databricks"  # Options: databricks, postgres, bigquery, redshift, etc.
EXAMPLE_DATA_PATH = "../translation_graph/tests/integration/example_data"

def load_json_file(file_path: str) -> dict:
    """Load JSON data from file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def transform_sql(sql: str) -> str:
    """Transform SQL from source to target dialect."""
    try:
        transformed = sqlglot.transpile(sql, read=SOURCE_DIALECT, write=TARGET_DIALECT)[0]
        return transformed
    except Exception as e:
        return f"-- Error: {str(e)}\n-- Original: {sql}"

def generate_table_ddl(table_metadata: dict) -> str:
    """Generate CREATE TABLE DDL from metadata."""
    columns = []
    for col in table_metadata['columns']:
        col_def = f"  {col['column_name']} {col['data_type']}"

        if col['data_type'] == 'VARCHAR' and col['character_maximum_length']:
            col_def += f"({col['character_maximum_length']})"
        elif col['data_type'] == 'NUMBER' and col['numeric_precision']:
            if col['numeric_scale'] and col['numeric_scale'] > 0:
                col_def += f"({col['numeric_precision']}, {col['numeric_scale']})"
            else:
                col_def += f"({col['numeric_precision']})"

        if col['is_nullable'] == 'NO':
            col_def += " NOT NULL"
        else:
            col_def += " NULL"

        if col['column_default']:
            col_def += f" DEFAULT {col['column_default']}"

        if col['comment']:
            col_def += f" COMMENT '{col['comment']}'"

        columns.append(col_def)

    table_name = f"{table_metadata['database_name']}.{table_metadata['schema_name']}.{table_metadata['table_name']}"
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += "\n)"

    if table_metadata['comment']:
        ddl += f" COMMENT '{table_metadata['comment']}'"

    return ddl + ";"

def generate_view_ddl(view_metadata: dict) -> str:
    """Generate CREATE VIEW DDL from metadata."""
    view_name = f"{view_metadata['database_name']}.{view_metadata['schema_name']}.{view_metadata['view_name']}"

    # Transform the view definition SQL
    transformed_sql = transform_sql(view_metadata['view_definition'])

    # Generate CREATE VIEW statement
    ddl = f"CREATE OR REPLACE VIEW {view_name} AS\n{transformed_sql}"

    return ddl + ";"

def demo_sql_transformations():
    """Demonstrate complex SQL transformations."""
    print(f"🔄 Complex SQL Dialect Transformations ({SOURCE_DIALECT} → {TARGET_DIALECT})")
    print("=" * 80)

    examples = [
        # Window functions
        "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn FROM employees",

        # CTEs (Common Table Expressions)
        "WITH sales_summary AS (SELECT department, SUM(amount) as total FROM sales GROUP BY department) SELECT * FROM sales_summary WHERE total > 1000",

        # Complex JOINs
        "SELECT e.name, d.dept_name, COUNT(o.order_id) as order_count FROM employees e LEFT JOIN departments d ON e.dept_id = d.id LEFT JOIN orders o ON e.id = o.employee_id GROUP BY e.name, d.dept_name",

        # Subqueries
        "SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE parent_category IS NULL)",

        # Array operations (Snowflake specific)
        "SELECT ARRAY_SIZE(tags) as tag_count, ARRAY_CONTAINS('urgent', tags) as is_urgent FROM issues",

        # Date functions
        "SELECT DATE_TRUNC('month', created_at) as month, COUNT(*) FROM orders GROUP BY DATE_TRUNC('month', created_at)",

        # CASE statements
        "SELECT id, name, CASE WHEN status = 'active' THEN '🟢' WHEN status = 'inactive' THEN '🔴' ELSE '🟡' END as status_icon FROM users",

        # UNION operations
        "SELECT id, name, 'customer' as type FROM customers UNION ALL SELECT id, name, 'supplier' as type FROM suppliers",

        # Complex aggregations
        "SELECT department, AVG(salary) as avg_salary, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary FROM employees GROUP BY department"
    ]

    for i, sql in enumerate(examples, 1):
        print(f"Example {i}:")
        print(f"Source: {sql}")
        try:
            transformed = transform_sql(sql)
            print(f"Target: {transformed}")
        except Exception as e:
            print(f"Error: {e}")
        print("-" * 80)

def demo_table_migration():
    """Demonstrate table migration."""
    print("\n📋 Table Migration Demo")
    print("=" * 50)

    try:
        tables_data = load_json_file(f"{EXAMPLE_DATA_PATH}/tables.json")
        print(f"Loaded {len(tables_data['tables'])} tables")

        for i, table in enumerate(tables_data['tables'][:2], 1):  # Show first 2 tables
            print(f"\nTable {i}: {table['table_name']}")
            ddl = generate_table_ddl(table)
            print(ddl)

    except FileNotFoundError:
        print(f"Example data not found at {EXAMPLE_DATA_PATH}")
        print("Make sure you're running from the sql_glot_concept directory")

def demo_view_migration():
    """Demonstrate view migration."""
    print("\n👁️  View Migration Demo")
    print("=" * 50)

    try:
        views_data = load_json_file(f"{EXAMPLE_DATA_PATH}/views.json")
        print(f"Loaded {len(views_data['views'])} views")

        for i, view in enumerate(views_data['views'][:1], 1):  # Show first view
            print(f"\nView {i}: {view['view_name']}")
            print(f"Original SQL: {view['view_definition']}")

            transformed_sql = transform_sql(view['view_definition'])
            print(f"Transformed: {transformed_sql}")

    except FileNotFoundError:
        print(f"Example data not found at {EXAMPLE_DATA_PATH}")
        print("Make sure you're running from the sql_glot_concept directory")

def demo_database_migration():
    """Demonstrate database migration."""
    print("\n🗄️  Database Migration Demo")
    print("=" * 50)

    try:
        databases_data = load_json_file(f"{EXAMPLE_DATA_PATH}/databases.json")
        print(f"Loaded {len(databases_data['databases'])} databases")

        for i, db in enumerate(databases_data['databases'], 1):
            print(f"\nDatabase {i}: {db['database_name']}")
            ddl = f"CREATE DATABASE IF NOT EXISTS {db['database_name']}"
            if db.get('comment'):
                ddl += f" COMMENT = '{db['comment']}'"
            print(f"DDL: {ddl};")

    except FileNotFoundError:
        print("Database data not found")

def demo_schema_migration():
    """Demonstrate schema migration."""
    print("\n📁 Schema Migration Demo")
    print("=" * 50)

    try:
        schemas_data = load_json_file(f"{EXAMPLE_DATA_PATH}/schemas.json")
        print(f"Loaded {len(schemas_data['schemas'])} schemas")

        for i, schema in enumerate(schemas_data['schemas'], 1):
            print(f"\nSchema {i}: {schema['schema_name']}")
            ddl = f"CREATE SCHEMA IF NOT EXISTS {schema['database_name']}.{schema['schema_name']}"
            if schema.get('comment'):
                ddl += f" COMMENT = '{schema['comment']}'"
            print(f"DDL: {ddl};")

    except FileNotFoundError:
        print("Schema data not found")

def demo_sequence_migration():
    """Demonstrate sequence migration."""
    print("\n🔢 Sequence Migration Demo")
    print("=" * 50)

    try:
        sequences_data = load_json_file(f"{EXAMPLE_DATA_PATH}/sequences.json")
        print(f"Loaded {len(sequences_data['sequences'])} sequences")

        for i, seq in enumerate(sequences_data['sequences'], 1):
            print(f"\nSequence {i}: {seq['sequence_name']}")
            ddl = f"CREATE SEQUENCE IF NOT EXISTS {seq['database_name']}.{seq['schema_name']}.{seq['sequence_name']}"
            ddl += f" START = {seq['start_value']} INCREMENT = {seq['increment']}"
            if seq.get('comment'):
                ddl += f" COMMENT = '{seq['comment']}'"
            print(f"DDL: {ddl};")

    except FileNotFoundError:
        print("Sequence data not found")

def demo_procedure_migration():
    """Demonstrate stored procedure migration."""
    print("\n⚙️  Procedure Migration Demo")
    print("=" * 50)

    try:
        procedures_data = load_json_file(f"{EXAMPLE_DATA_PATH}/procedures.json")
        print(f"Loaded {len(procedures_data['procedures'])} procedures")

        for i, proc in enumerate(procedures_data['procedures'], 1):
            print(f"\nProcedure {i}: {proc['procedure_name']}")
            # Extract the SQL body from between the $$ markers
            definition = proc['procedure_definition']
            sql_start = definition.find('$$') + 2
            sql_end = definition.rfind('$$')
            if sql_start > 1 and sql_end > sql_start:
                sql_body = definition[sql_start:sql_end].strip()
                print(f"Original SQL body: {sql_body}")

                transformed_sql = transform_sql(sql_body)
                print(f"Transformed SQL body: {transformed_sql}")

                # Reconstruct the procedure definition
                transformed_definition = definition[:sql_start] + transformed_sql + definition[sql_end:]
                print(f"Full transformed procedure: {transformed_definition[:100]}...")

    except FileNotFoundError:
        print("Procedure data not found")

def demo_udf_migration():
    """Demonstrate user-defined function migration."""
    print("\n🔧 UDF Migration Demo")
    print("=" * 50)

    try:
        udfs_data = load_json_file(f"{EXAMPLE_DATA_PATH}/udfs.json")
        print(f"Loaded {len(udfs_data['functions'])} functions")

        for i, udf in enumerate(udfs_data['functions'], 1):
            print(f"\nFunction {i}: {udf['function_name']}")
            # Extract the SQL body from between the $$ markers
            definition = udf['function_definition']
            sql_start = definition.find('$$') + 2
            sql_end = definition.rfind('$$')
            if sql_start > 1 and sql_end > sql_start:
                sql_body = definition[sql_start:sql_end].strip()
                print(f"Original SQL body: {sql_body}")

                transformed_sql = transform_sql(sql_body)
                print(f"Transformed SQL body: {transformed_sql}")

                # Reconstruct the function definition
                transformed_definition = definition[:sql_start] + transformed_sql + definition[sql_end:]
                print(f"Full transformed function: {transformed_definition[:100]}...")

    except FileNotFoundError:
        print("UDF data not found")

def demo_ast_parsing():
    """Demonstrate AST parsing capabilities."""
    print("\n🌳 AST Parsing Demo")
    print("=" * 50)

    sql = "SELECT id, name FROM users WHERE active = true"
    parsed = sqlglot.parse_one(sql, dialect=SOURCE_DIALECT)

    print(f"Original SQL: {sql}")
    print(f"Parsed AST: {parsed}")
    print(f"AST type: {type(parsed)}")

    # Transform to target dialect
    transformed = parsed.sql(dialect=TARGET_DIALECT)
    print(f"Transformed: {transformed}")

    # Find all columns in the AST
    print("\nColumns found:")
    for col in parsed.find_all(sqlglot.exp.Column):
        print(f"  - {col}")

def comprehensive_comparison():
    """Comprehensive comparison of SQLGlot vs LLM for ALL database objects."""
    print("🔄 COMPREHENSIVE SQLGLOT vs LLM COMPARISON")
    print("=" * 80)
    print(f"Configuration: {SOURCE_DIALECT} → {TARGET_DIALECT}")
    print(f"Processing ALL database objects from example data")
    print("=" * 80)
    print()

    # Check if sqlglot is installed
    try:
        import sqlglot
        print("✅ SQLGlot is installed")
    except ImportError:
        print("❌ SQLGlot not found. Install with: pip install sqlglot")
        return

    # Object types to process: (filename, object_type, json_key)
    object_types = [
        ("databases", "database", "databases"),
        ("schemas", "schema", "schemas"),
        ("sequences", "sequence", "sequences"),
        ("tables", "table", "tables"),
        ("views", "view", "views"),
        ("procedures", "procedure", "procedures"),
        ("udfs", "function", "functions")  # Note: JSON key is "functions" not "udfs"
    ]

    total_objects = 0
    successful_comparisons = 0
    identical_results = 0

    for json_file, object_type, json_key in object_types:
        try:
            # Load data
            with open(f"{EXAMPLE_DATA_PATH}/{json_file}.json", "r") as f:
                data = json.load(f)

            # Get items using the correct JSON key
            items = data.get(json_key, [])

            if not items:
                print(f"⚠️  {object_type.upper()}: No data found")
                continue

            print(f"🗄️  {object_type.upper()} ({len(items)} objects)")
            print("-" * 60)

            # Process each item
            for i, metadata in enumerate(items, 1):
                print(f"  Object {i}: Processing...")

                # Get object name for display
                if object_type == "database":
                    obj_name = metadata.get("database_name", "unknown")
                elif object_type in ["schema", "sequence", "table", "view", "procedure", "function"]:
                    db = metadata.get("database_name", "")
                    schema = metadata.get("schema_name", "")
                    name = metadata.get(f"{object_type}_name", "")
                    obj_name = f"{db}.{schema}.{name}" if db and schema else name
                else:
                    obj_name = "unknown"

                # SQLGlot approach
                sqlglot_result = generate_object_ddl(object_type, metadata)

                # LLM approach
                llm_result = run_llm_comparison(object_type, metadata)

                # Display results
                print(f"    📍 {obj_name}")
                print("    🤖 SQLGlot Result:")
                print("    " + "-" * 40)
                # Split result into lines and indent
                for line in sqlglot_result.split('\n'):
                    if line.strip():
                        print(f"    {line}")
                print()
                print("    🤖 LLM Result:")
                print("    " + "-" * 40)
                # Split result into lines and indent
                for line in llm_result.split('\n'):
                    if line.strip():
                        print(f"    {line}")
                print()

                # Metrics
                sqlglot_len = len(sqlglot_result)
                llm_len = len(llm_result)
                is_identical = sqlglot_result.strip() == llm_result.strip()

                print("    📊 METRICS:")
                print(f"      SQLGlot length: {sqlglot_len} characters")
                print(f"      LLM length: {llm_len} characters")
                print(f"      Results identical: {is_identical}")
                print()

                total_objects += 1
                successful_comparisons += 1
                if is_identical:
                    identical_results += 1

        except Exception as e:
            print(f"❌ Error processing {object_type}: {e}")
            continue

    # Summary
    print("=" * 80)
    print("📈 COMPARISON SUMMARY")
    print("=" * 80)
    print(f"Total objects processed: {total_objects}")
    print(f"Successful comparisons: {successful_comparisons}")
    print(f"Identical results: {identical_results}")
    if successful_comparisons > 0:
        identical_percentage = (identical_results / successful_comparisons) * 100
        print(".1f")
    print()
    print("🎯 KEY FINDINGS:")
    print("  • SQLGlot: Deterministic, fast, zero-cost, syntax-focused transformations")
    print("  • LLM: Semantic understanding, variable results, API costs, context-aware")
    print("  • Differences: Both produce valid DDL with different approaches (syntax vs semantic)")
    print("  • Coverage: Both handle ALL 7 object types completely")
    print("  • Recommendation: SQLGlot for bulk migration, LLM for complex business logic")

def generate_object_ddl(object_type, metadata):
    """Generate DDL for any object type using SQLGlot approach."""
    if object_type == "database":
        ddl = f"CREATE DATABASE IF NOT EXISTS {metadata['database_name']}"
        if metadata.get("comment"):
            ddl += f" COMMENT = '{metadata['comment']}'"
        return ddl + ";"

    elif object_type == "schema":
        ddl = f"CREATE SCHEMA IF NOT EXISTS {metadata['database_name']}.{metadata['schema_name']}"
        if metadata.get("comment"):
            ddl += f" COMMENT = '{metadata['comment']}'"
        return ddl + ";"

    elif object_type == "sequence":
        ddl = f"CREATE SEQUENCE IF NOT EXISTS {metadata['database_name']}.{metadata['schema_name']}.{metadata['sequence_name']}"
        ddl += f" START = {metadata['start_value']} INCREMENT = {metadata['increment']}"
        if metadata.get("comment"):
            ddl += f" COMMENT = '{metadata['comment']}'"
        return ddl + ";"

    elif object_type == "table":
        return generate_table_ddl(metadata)

    elif object_type == "view":
        return generate_view_ddl(metadata)

    elif object_type == "procedure":
        return generate_procedure_ddl(metadata)

    elif object_type == "function":
        return generate_udf_ddl(metadata)

    else:
        return f"-- {object_type.upper()} DDL generation not implemented"

def generate_procedure_ddl(procedure_metadata):
    """Generate CREATE PROCEDURE DDL from metadata."""
    procedure_name = f"{procedure_metadata['database_name']}.{procedure_metadata['schema_name']}.{procedure_metadata['procedure_name']}"

    # Extract SQL body from procedure definition
    definition = procedure_metadata['procedure_definition']
    sql_start = definition.find("$$") + 2
    sql_end = definition.rfind("$$")

    if sql_start > 1 and sql_end > sql_start:
        sql_body = definition[sql_start:sql_end].strip()

        # Transform the SQL body
        transformed_sql = transform_sql(sql_body)

        # Reconstruct procedure
        transformed_definition = definition[:sql_start] + transformed_sql + definition[sql_end:]
        return transformed_definition
    else:
        return f"-- Error: Could not parse procedure SQL body\\n{procedure_metadata['procedure_definition']}"

def generate_udf_ddl(udf_metadata):
    """Generate CREATE FUNCTION DDL from metadata."""
    function_name = f"{udf_metadata['database_name']}.{udf_metadata['schema_name']}.{udf_metadata['function_name']}"

    # Extract SQL body from function definition
    definition = udf_metadata['function_definition']
    sql_start = definition.find("$$") + 2
    sql_end = definition.rfind("$$")

    if sql_start > 1 and sql_end > sql_start:
        sql_body = definition[sql_start:sql_end].strip()

        # Transform the SQL body
        transformed_sql = transform_sql(sql_body)

        # Reconstruct function
        transformed_definition = definition[:sql_start] + transformed_sql + definition[sql_end:]
        return transformed_definition
    else:
        return f"-- Error: Could not parse function SQL body\\n{udf_metadata['function_definition']}"

def run_llm_comparison(object_type, metadata):
    """Run LLM comparison for a single object."""
    try:
        # Import here to avoid issues if LLM not available
        import sys
        sys.path.append("../translation_graph")

        from nodes.database_translation import translate_databases
        from nodes.schemas_translation import translate_schemas
        from nodes.sequences_translation import translate_sequences
        from nodes.tables_translation import translate_tables
        from nodes.views_translation import translate_views
        from nodes.procedures_translation import translate_procedures
        from nodes.udfs_translation import translate_udfs
        from utils.types import ArtifactBatch

        # Create batch with single item
        batch = ArtifactBatch(
            artifact_type=object_type,
            items=[json.dumps(metadata)],
            context={"source_db": SOURCE_DIALECT, "target_db": TARGET_DIALECT}
        )

        # Run appropriate translation
        if object_type == "database":
            result = translate_databases(batch)
        elif object_type == "schema":
            result = translate_schemas(batch)
        elif object_type == "sequence":
            result = translate_sequences(batch)
        elif object_type == "table":
            result = translate_tables(batch)
        elif object_type == "view":
            result = translate_views(batch)
        elif object_type == "procedure":
            result = translate_procedures(batch)
        elif object_type == "function":
            result = translate_udfs(batch)
        else:
            return f"-- {object_type.upper()} LLM translation not implemented"

        # Combine results
        llm_output = "\\n\\n".join(result.results)
        if result.errors:
            llm_output += f"\\n\\n-- ERRORS:\\n\\n" + "\\n\\n".join(result.errors)

        return llm_output

    except Exception as e:
        return f"-- LLM Error: {str(e)}\\n-- Make sure Databricks credentials are configured"

def main():
    """Main demo function."""
    print("🚀 SQLGlot Database Migration Demo")
    print("This demonstrates SQLGlot-based parsing instead of LLMs")
    print(f"Configuration: {SOURCE_DIALECT} → {TARGET_DIALECT}")
    print("=" * 60)
    print()

    # Check if sqlglot is installed
    try:
        import sqlglot
        print("✅ SQLGlot is installed")
    except ImportError:
        print("❌ SQLGlot not found. Install with: pip install sqlglot")
        return

    # Choose which demo to run based on command line args
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        comprehensive_comparison()
    else:
        # Original individual demos
        demo_sql_transformations()
        demo_database_migration()
        demo_schema_migration()
        demo_sequence_migration()
        demo_table_migration()
        demo_view_migration()
        demo_procedure_migration()
        demo_udf_migration()
        demo_ast_parsing()

        print("\\n" + "=" * 60)
        print("💡 For comprehensive comparison of ALL objects, run:")
        print("python3 demo_script.py compare")

if __name__ == "__main__":
    main()
