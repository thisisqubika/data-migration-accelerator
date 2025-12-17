# Artifact Translation Package

This package provides a complete solution for migrating Snowflake database artifacts to Databricks using LLM-based translation with built-in validation, observability, and error handling.

## Features

- **Multi-Artifact Support**: Translates databases, schemas, tables, views, procedures, UDFs, and more
- **Intelligent Routing**: Automatically routes artifacts to appropriate translation nodes
- **SQL Validation**: Built-in syntax validation using SQLGlot and LLM-based evaluation
- **Observability**: Comprehensive logging, metrics, and error tracking
- **Batch Processing**: Efficiently processes large numbers of artifacts
- **Local Testing**: Run and test translations locally before deploying to Databricks
- **Output Flexibility**: Generate SQL, JSON, or combined output formats

## Installation

```bash
# Install from the workspace root
pip install -r requirements.txt
```

## Quick Start

### Local Execution

Process JSON files containing Snowflake artifact definitions. The CLI accepts one
or more JSON files and a new `--output-format` option (`sql`, `json`, `combined`).

```bash
# Single file (SQL output only)
PYTHONPATH=src python3 -m artifact_translation_package.main \
  path/to/tables.json \
  --batch-size 5 \
  --output ./output \
  --output-format sql

# Multiple files (both JSON and SQL outputs)
PYTHONPATH=src python3 -m artifact_translation_package.main \
  path/to/tables.json \
  path/to/views.json \
  path/to/schemas.json \
  --batch-size 5 \
  --output ./output \
  --output-format combined
```

### Programmatic Usage

```python
from artifact_translation_package.graph_builder import build_translation_graph
from artifact_translation_package.utils.file_processor import create_batches_from_file

# Build the translation graph
graph = build_translation_graph()

# Create batches from a JSON file
batches = create_batches_from_file("tables.json", batch_size=10)

# Process batches
for batch in batches:
    result = graph.run(batch)
    print(f"Processed {len(result['tables'])} tables")
```

## Running Tests

### Unit Tests

```bash
# Run from workspace root
PYTHONPATH=src python3 -m pytest src/artifact_translation_package/tests/test_evaluator.py -v
```

### Integration Tests

```bash
# Test full workflow
PYTHONPATH=src python3 -m pytest src/artifact_translation_package/tests/integration/test_full_workflow.py -v

# Test main.py processing
PYTHONPATH=src python3 -m pytest src/artifact_translation_package/tests/integration/test_main.py -v
```

### Manual Testing with Example Data

Use the included example JSONs to test locally. Example commands below produce
SQL, JSON, or both depending on `--output-format`.

```bash
# SQL only
PYTHONPATH=src python3 -m artifact_translation_package.main \
  src/artifact_translation_package/tests/integration/example_data/tables.json \
  --batch-size 2 \
  --output ./test_output \
  --output-format sql

# Combined (JSON + SQL) for multiple files
PYTHONPATH=src python3 -m artifact_translation_package.main \
  src/artifact_translation_package/tests/integration/example_data/tables.json \
  src/artifact_translation_package/tests/integration/example_data/views.json \
  src/artifact_translation_package/tests/integration/example_data/schemas.json \
  --batch-size 5 \
  --output ./test_output \
  --output-format combined
```

Quick one-liner to run and inspect outputs immediately:

```bash
PYTHONPATH=src python3 -m artifact_translation_package.main \
  src/artifact_translation_package/tests/integration/example_data/views.json \
  --batch-size 2 \
  --output ./quick_test \
  --output-format combined && echo "Outputs in ./quick_test" 
```

## Using run_file_processor.py

The `run_file_processor.py` script provides an alternative way to process files with more control over output:

```bash
PYTHONPATH=src python3 -m artifact_translation_package.run_file_processor \
  src/artifact_translation_package/tests/integration/example_data/tables.json \
  --batch-size 2 \
  --output ./output \
  --output-format sql
```

## Configuration

### LLM Configuration

Edit `config/ddl_config.py` to configure LLM providers and models:

```python
DDL_TRANSLATION_CONFIG = {
    "llm": {
        "provider": "databricks",  # or "openai", "anthropic"
        "model": "databricks-meta-llama-3-1-70b-instruct",
        # Add API keys and endpoints as needed
    },
    "processing": {
        "batch_size": 10,
        "evaluation_batch_size": 5
    },
    "validation": {
        "enabled": True,
        "llm_validated_artifacts": ["procedures", "pipes"],
        "skip_unsupported_artifacts": []
    }
}
```

### Environment Variables

Create a `.env` file or set environment variables:

```bash
# LLM Configuration
DATABRICKS_HOST=your-databricks-workspace-url
DATABRICKS_TOKEN=your-databricks-token

# Optional: Output directory
DDL_OUTPUT_DIR=./output
```

## Architecture

### Translation Graph

The package uses a LangGraph-based translation pipeline:

```
Input JSON → Router → Translation Node → Evaluation Node → Aggregator → Output
                ↓
        (tables, views, procedures, etc.)
```

### Key Components

- **Nodes**: Translation logic for each artifact type ([nodes/](nodes/))
- **Prompts**: LLM prompts for each artifact ([prompts/](prompts/))
- **Utils**: Helper functions and utilities ([utils/](utils/))
  - `file_processor.py`: File and batch processing
  - `llm_utils.py`: LLM interaction
  - `llm_evaluation_utils.py`: LLM-based SQL validation
  - `observability.py`: Logging and metrics
  - `error_handler.py`: Error handling with retries
- **Config**: Configuration management ([config/](config/))
- **Tests**: Unit and integration tests ([tests/](tests/))

## Output Structure

### SQL Format

Generated SQL files are organized by artifact type:

```
output/
├── databases.sql
├── schemas.sql
├── tables.sql
├── views.sql
├── procedures.sql
└── evaluation_results/
    └── evaluation_batch_tables_20250116_143022.json
```

### JSON Format

```json
{
  "tables": ["CREATE TABLE ...", "CREATE TABLE ..."],
  "views": ["CREATE VIEW ...", "CREATE VIEW ..."],
  "metadata": {
    "total_results": 10,
    "errors": [],
    "processing_stats": {...},
    "evaluation_results_count": 0
  }
}
```

### Evaluation Results

Validation results are stored in `evaluation_results/`:

```json
{
  "batch": {
    "artifact_type": "tables",
    "evaluated_count": 5
  },
  "validation": {
    "total_statements": 5,
    "valid_statements": 4,
    "invalid_statements": 1,
    "validation_method": "syntax_validator"
  },
  "timestamp": "20250116_143022"
}
```

## Deployment to Databricks

### Using Databricks Jobs

1. **Package the code**:
   ```bash
   # From workspace root
   tar -czf artifact_translation_package.tar.gz src/artifact_translation_package/
   ```

2. **Upload to DBFS**:
   ```bash
   databricks fs cp artifact_translation_package.tar.gz dbfs:/FileStore/packages/
   ```

3. **Create a Databricks Job**:
   Use `databricks_job.py` as the entry point.

### Using databricks_job.py

The `databricks_job.py` file is designed to run on Databricks:

```python
# Example job configuration
{
  "name": "Artifact Translation Job",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "libraries": [
    {"pypi": {"package": "langchain"}},
    {"pypi": {"package": "langgraph"}},
    {"pypi": {"package": "pydantic"}},
    {"pypi": {"package": "sqlglot"}}
  ],
  "spark_python_task": {
    "python_file": "dbfs:/FileStore/artifact_translation_package/databricks_job.py",
    "parameters": [
      "--input-path", "dbfs:/input/tables.json",
      "--output-path", "dbfs:/output/",
      "--batch-size", "10"
    ]
  }
}
```

## Troubleshooting

### Import Errors

If you encounter import errors, ensure you're running from the workspace root with `PYTHONPATH=src`:

```bash
# Correct
PYTHONPATH=src python3 -m artifact_translation_package.main ...

# Incorrect (will fail)
cd src/artifact_translation_package && python3 main.py ...
```

### LLM Connection Issues

Check your LLM configuration:
- Verify API keys are set correctly
- Check network connectivity
- Review LLM provider documentation

### Validation Failures

If SQL validation fails:
- Review evaluation results in `evaluation_results/`
- Check artifact complexity (procedures may need LLM validation)
- Adjust validation settings in `config/ddl_config.py`

## Contributing

When adding new features:
1. Add unit tests in `tests/`
2. Update integration tests if needed
3. Document new configuration options
4. Update this README

## License

Copyright (c) 2025. All rights reserved.
