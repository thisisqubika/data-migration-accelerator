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
│   └── llm_utils.py           # LLM creation utilities
├── tests/
│   └── test_graph.py          # Test suite
├── graph_builder.py           # LangGraph construction
├── run_example.py             # Example usage
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

### Basic Example

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

## Testing

Run the test suite:

```bash
python -m pytest tests/test_graph.py -v
```

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

## Requirements

- Python 3.7+
- LangChain ecosystem
- OpenAI API or Databricks endpoint access
