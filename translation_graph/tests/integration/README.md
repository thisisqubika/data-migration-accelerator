# Integration Tests and Examples

This directory contains integration tests and example notebooks that demonstrate how to use the Translation Graph.

## Files

- **`test_integration_example.py`** - Complete integration test that runs the translation graph with example JSON files
- **`run_example.py`** - Basic example showing how to use the graph programmatically
- **`databricks_job_notebook.py`** - Databricks notebook for parameterized jobs
- **`databricks_notebook_example.py`** - Complete Databricks notebook example

## Example Data

The `example_data/` directory contains sample JSON files:
- `tables.json` - Sample table definitions
- `views.json` - Sample view definitions
- `schemas.json` - Sample schema definitions

## Running Integration Tests

### Run the full integration test suite:

```bash
cd translation_graph
python tests/integration/test_integration_example.py
```

This will:
1. Load example JSON files from `example_data/`
2. Process them through the translation graph
3. Validate the results
4. Save output files (if configured)

### Run individual examples:

```bash
# Basic example
python tests/integration/run_example.py

# Integration test with example files
python tests/integration/test_integration_example.py
```

## Example Data Structure

The example JSON files follow the same structure as real Snowflake object exports:

```json
{
  "tables": [
    {
      "database_name": "DATA_MIGRATION_DB",
      "schema_name": "DATA_MIGRATION_SCHEMA",
      "table_name": "EXAMPLE_TABLE_1",
      "table_type": "BASE TABLE",
      "columns": [...]
    }
  ]
}
```

## Output

When running `test_integration_example.py`, results are saved to:
- `example_data/output/translation_results.json`

## Requirements

Make sure you have:
1. All dependencies installed (`pip install -r requirements.txt`)
2. Databricks LLM endpoint configured (set `DBX_ENDPOINT` environment variable)
3. Example data files in `example_data/` directory

## Notes

- These tests use example data and don't require actual database connections
- The tests demonstrate the full workflow from JSON files to translated DDL
- Results are validated but actual DDL execution is optional


