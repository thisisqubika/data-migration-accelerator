# DDL Translation Graph

A LangGraph-based system for translating DDL (Data Definition Language) statements between different database systems using Large Language Models.

## Project Structure

```
translation_graph/
├── config/
│   └── ddl_config.py          # Configuration management
├── nodes/
│   ├── router.py              # Smart routing logic
│   ├── *_translation.py       # Individual translation nodes
│   └── aggregator.py          # Result aggregation
├── prompts/
│   └── *_prompts.py           # Prompt templates for each node
├── utils/
│   ├── types.py               # Shared data types
│   ├── llm_utils.py           # LLM creation utilities
│   └── file_processor.py      # File-based processing utilities
├── docs/
│   ├── DATABRICKS.md          # Databricks deployment guide
│   └── README.md              # Documentation index
├── tests/
│   └── integration/
│       ├── run_example.py             # Basic example usage
│       ├── databricks_job_notebook.py # Parameterized job notebook
│       └── databricks_notebook_example.py  # Example Databricks notebook
├── graph_builder.py           # LangGraph construction
├── main.py                    # Main entry point for file-based processing
├── databricks_job.py          # Databricks job entry point
└── requirements.txt           # Python dependencies
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```bash
   # Copy the .env file and update with your values
   cp .env .env.local  # Edit .env.local with your credentials

   # Required: Databricks LLM endpoint
   export DBX_ENDPOINT="databricks-llama-4-maverick"

   # Optional: Batch processing settings
   export DDL_BATCH_SIZE=8
   export DDL_OUTPUT_DIR=./ddl_output

   # Optional: Feature flags
   export DDL_ENABLE_MLFLOW=true
   export DDL_VERBOSE_LOGGING=true
   ```

   **Environment Variables:**
   - `DBX_ENDPOINT`: Databricks LLM endpoint (required)
   - `DDL_BATCH_SIZE`: Processing batch size (default: 8)
   - `DDL_OUTPUT_DIR`: Output directory (default: ./ddl_output)
   - `DDL_TEMPERATURE`: LLM temperature (default: 0.1)
   - `DDL_MAX_TOKENS`: Max tokens per request (default: 2000)
   - `DDL_ENABLE_MLFLOW`: Enable MLflow tracking (default: true)
   - `DDL_VERBOSE_LOGGING`: Enable verbose logging (default: true)

## LLM Configuration

The system uses Databricks LLMs exclusively for pipeline execution:

### Default Configuration
```python
from config.ddl_config import DDLConfig

config = DDLConfig()
# Uses Databricks databricks-llama-4-maverick by default
```

### Custom Endpoint
```python
from config.ddl_config import DDLConfig

config = DDLConfig({
    "llms": {
        "smart_router": {
            "provider": "databricks",
            "model": "databricks-llama-4-maverick",
            "temperature": 0.1,
            "additional_params": {
                "endpoint": "your-custom-endpoint"
            }
        }
    }
})
```

## Usage

### File-Based Processing (Recommended)

Process JSON files where each file contains a specific artifact type. The artifact type is determined from the filename (e.g., `tables.json`, `views.json`).

```bash
# Process a single file
python main.py tables.json --batch-size 10

# Process multiple files
python main.py tables.json views.json schemas.json --batch-size 10

# Save results to a file
python main.py tables.json --batch-size 10 --output results.json
```

**JSON File Structure:**
Each JSON file should have a top-level key matching the artifact type:
```json
{
  "tables": [
    {"database_name": "...", "schema_name": "...", "table_name": "...", ...},
    ...
  ]
}
```

**Filename conventions:**
- `tables.json`, `table.json` → tables
- `views.json`, `view.json` → views
- `schemas.json`, `schema.json` → schemas
- `procedures.json`, `procedure.json` → procedures
- `roles.json`, `role.json` → roles
- And similar patterns for other artifact types

**Batching:**
- Large files are automatically split into batches
- Each batch is processed separately and results are aggregated
- Default batch size is 10 artifacts per batch
- Adjust with `--batch-size` parameter

### Programmatic Usage

```python
from graph_builder import build_translation_graph
from utils.file_processor import create_batches_from_file

# Process a JSON file with batching
graph = build_translation_graph()
batches = create_batches_from_file("tables.json", batch_size=10)

# Process all batches
result = graph.run_batches(batches)
print(result)
```

### Basic Example (Direct Batch)

```python
from graph_builder import build_translation_graph
from utils.types import ArtifactBatch

# Create translation graph
graph = build_translation_graph()

# Create artifact batch
batch = ArtifactBatch(
    artifact_type="tables",
    items=["CREATE TABLE users (id INT, name VARCHAR(255))"],
    context={"source_db": "snowflake", "target_db": "postgres"}
)

# Run translation
result = graph.run(batch)
print(result)
```

### LLM Utilities

```python
from utils.llm_utils import create_llm_for_node, validate_node_requirements

# Create LLM for specific node
llm = create_llm_for_node("tables_translator")

# Validate system requirements
validation = validate_node_requirements()
print(f"System ready: {validation['environment_ready']}")
```

## Node Types

- **Router**: Routes artifacts to appropriate translation nodes
- **Tables Translator**: Translates table DDL
- **Views Translator**: Translates view DDL
- **Schemas Translator**: Translates schema DDL
- **Procedures Translator**: Translates stored procedure DDL
- **Roles Translator**: Translates role/permission DDL
- **Aggregator**: Merges results from all translation nodes

## Configuration

The system uses a hierarchical configuration system with:

- Default configurations
- Environment variable overrides
- Per-node LLM settings
- File-based configuration support

See `config/ddl_config.py` for detailed configuration options.

## Testing and Examples

### Integration Tests

Run the comprehensive integration test with example data:

```bash
# Run full workflow test with example JSON files
python tests/integration/test_full_workflow.py
```

This comprehensive test demonstrates:
- Loading example JSON files from `tests/integration/example_data/`
- Processing tables, views, and schemas through the translation graph
- Real LLM-powered SQL DDL generation
- Result validation and persistence

### File Processor Example

Process your own JSON files directly:

```bash
# Process JSON files with the file processor
python main.py tables.json --batch-size 10

# Generate SQL files instead of JSON
python databricks_job.py --input-files tables.json --output-format sql --output-path ./output/
```

### Example Data

Sample JSON files are provided in `tests/integration/example_data/`:
- `tables.json` - Sample table definitions (2 tables)
- `views.json` - Sample view definitions (2 views)
- `schemas.json` - Sample schema definitions (1 schema)

See [tests/integration/README.md](tests/integration/README.md) for complete documentation.

## Development

To implement real functionality:

1. Update prompt templates in `prompts/*.py`
2. Implement LLM calls in translation nodes using `create_llm_for_node()`
3. Add real routing logic in `nodes/router.py`
4. Update the graph builder to use actual LangGraph components

## Error Handling

The system includes comprehensive error handling:

- LLM creation failures
- Configuration validation
- Translation errors are captured and reported
- Graceful degradation with error metadata

## Databricks Deployment

For using this translation graph in Databricks jobs and pipelines, see the [Databricks Deployment Guide](docs/DATABRICKS.md).

### Quick Start for Databricks

```python
from translation_graph.databricks_job import process_translation_job

result = process_translation_job(
    input_files=["dbfs:/FileStore/data/tables.json"],
    output_path="dbfs:/FileStore/results/results.json",
    batch_size=10
)
```

### Key Features for Databricks

- **DBFS and Volume support**: Handles `dbfs:/` and `/Volumes/` paths automatically
- **Batch processing**: Processes large files in configurable batches
- **Job integration**: Ready-to-use entry points for Databricks jobs
- **Pipeline support**: Compatible with Databricks Pipelines
- **Unity Catalog**: Works with Unity Catalog Volumes

See [docs/DATABRICKS.md](docs/DATABRICKS.md) for complete deployment instructions.

## Requirements

- Python 3.7+
- LangChain ecosystem
- Databricks endpoint access (for LLM calls)
