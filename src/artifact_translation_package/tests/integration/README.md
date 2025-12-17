# Integration Tests and Examples

This directory contains integration tests and example JSON files for the translation graph.

## Files

- `test_integration_example.py` – integration test that runs the translation graph with example JSON files
- `run_example.py` – small programmatic example

## Example Data

The `example_data/` directory contains sample JSON files used by the tests:
- `tables.json`
- `views.json`
- `schemas.json`

## Running Locally (recommended)

Add the `src` package to `PYTHONPATH` and run the module entrypoints. Examples below assume you're at the repository root.

Run the full integration test file with pytest:

```bash
PYTHONPATH=src python3 -m pytest src/artifact_translation_package/tests/integration/test_full_workflow.py -q
```

Run the translation graph programmatically (single-file):

```bash
PYTHONPATH=src python3 -m artifact_translation_package.main \
  src/artifact_translation_package/tests/integration/example_data/tables.json \
  --batch-size 2 --output ./test_output --output-format combined
```

Or use the file processor runner which accepts multiple files:

```bash
PYTHONPATH=src python3 -m artifact_translation_package.run_file_processor \
  src/artifact_translation_package/tests/integration/example_data/tables.json \
  src/artifact_translation_package/tests/integration/example_data/views.json \
  src/artifact_translation_package/tests/integration/example_data/schemas.json \
  --batch-size 2 --output ./test_output --output-format sql
```

Notes:
- `--output-format` supports: `sql`, `json`, `combined`.
- Outputs are written to a timestamped results folder under the `--output` directory (e.g. `./test_output/results_<timestamp>/`).
- The CLI expects artifact-type logical names (e.g. `grants`) — input JSON files may use keys like `grants_flattened` (the file processor maps these automatically).

## Example JSON structure

Files follow the Snowflake export style, e.g.:

```json
{
  "tables": [
    {
      "database_name": "DATA_MIGRATION_DB",
      "schema_name": "DATA_MIGRATION_SCHEMA",
      "table_name": "EXAMPLE_TABLE_1",
      "table_type": "BASE TABLE",
      "columns": []
    }
  ]
}
```

## Requirements

1. Install dependencies: `pip install -r requirements.txt`
2. Configure any LLM or Databricks credentials required for evaluation (if running those nodes).

## Troubleshooting

- If you see a KeyError about `grants` vs `grants_flattened`, the file processor already maps `grants` → `grants_flattened`. Ensure your JSON uses `grants_flattened` or run the grant-flattening step in `migration_accelerator_package` first.
- If imports fail when running tests, run with `PYTHONPATH=src` as shown above.


