# Databricks Deployment Guide

Complete guide for deploying and using the Translation Graph in Databricks jobs and pipelines.

## Quick Start

### Installation

**Option 1: Upload to DBFS (Quickest)**
```bash
cd translation_graph
zip -r translation_graph.zip . -x "*.pyc" "__pycache__/*" "*.git*" "*.md"
databricks fs cp translation_graph.zip dbfs:/FileStore/libraries/translation_graph.zip
```
Then install in cluster: Libraries → Install New → DBFS → `translation_graph.zip`

**Option 2: Use Repos (Recommended for CI/CD)**
1. Workspace → Repos → Add Repo → Connect Git repository
2. Files are automatically available in notebooks

**Option 3: Direct Upload**
```python
# Upload translation_graph folder to DBFS or Volume, then:
import sys
sys.path.insert(0, '/dbfs/FileStore/translation_graph')
# or
sys.path.insert(0, '/Volumes/catalog/schema/volume_name/translation_graph')
```

### Basic Usage

```python
from translation_graph.databricks_job import process_translation_job

result = process_translation_job(
    input_files=["dbfs:/FileStore/data/tables.json"],
    output_path="dbfs:/FileStore/results/results.json",
    batch_size=10
)
```

## Deployment Options

### In Databricks Notebooks

```python
# Setup
import sys
sys.path.insert(0, '/dbfs/FileStore/translation_graph')

from translation_graph.databricks_job import process_translation_job

# Process files
input_files = [
    "dbfs:/FileStore/data/tables.json",
    "dbfs:/FileStore/data/views.json"
]

result = process_translation_job(
    input_files=input_files,
    output_path="dbfs:/FileStore/results/translation_results.json",
    batch_size=10
)

# Display results
print(f"Total results: {result['metadata']['total_results']}")
```

### In Databricks Jobs

1. **Create Job:**
   - Jobs → Create Job → Notebook Task
   - Notebook: `/Workspace/Users/your_email/translation_job_notebook`

2. **Notebook Code:**
   ```python
   # Get parameters from job widgets
   input_files = [f.strip() for f in dbutils.widgets.get("input_files").split(",")]
   output_path = dbutils.widgets.get("output_path")
   batch_size = int(dbutils.widgets.get("batch_size", "10"))
   
   from translation_graph.databricks_job import process_translation_job
   
   result = process_translation_job(
       input_files=input_files,
       output_path=output_path,
       batch_size=batch_size
   )
   
   print(f"Translation completed: {result['metadata']['total_results']} results")
   ```

3. **Job Parameters:**
   - `input_files`: `dbfs:/FileStore/data/tables.json,dbfs:/FileStore/data/views.json`
   - `output_path`: `dbfs:/FileStore/results/results.json`
   - `batch_size`: `10`

### In Databricks Pipelines

```python
from translation_graph.databricks_job import process_translation_job

# Get pipeline parameters
input_files = spark.conf.get("input_files", "").split(",")
output_path = spark.conf.get("output_path")
batch_size = int(spark.conf.get("batch_size", "10"))

# Process translation
result = process_translation_job(
    input_files=input_files,
    output_path=output_path,
    batch_size=batch_size
)

# Save to Delta table (optional)
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

rows = []
for artifact_type, artifacts in result.items():
    if artifact_type != "metadata" and isinstance(artifacts, list):
        for artifact in artifacts:
            rows.append(Row(
                artifact_type=artifact_type,
                artifact_ddl=artifact,
                processed_at=current_timestamp()
            ))

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").saveAsTable("translation_results")
```

## Common Usage Patterns

### Process from Volume

```python
from translation_graph.databricks_job import process_from_volume

result = process_from_volume(
    volume_path="/Volumes/main/default/migration_data/",
    artifact_types=["tables", "views"],  # Optional filter
    output_path="dbfs:/FileStore/results/results.json",
    batch_size=10
)
```

### Process Multiple Files

```python
input_files = [
    "dbfs:/FileStore/data/tables.json",
    "dbfs:/FileStore/data/views.json",
    "dbfs:/FileStore/data/schemas.json"
]

result = process_translation_job(
    input_files=input_files,
    output_path="dbfs:/FileStore/results/all_results.json",
    batch_size=10
)
```

### Save Results to Delta Table

```python
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

rows = []
for artifact_type, artifacts in result.items():
    if artifact_type != "metadata" and isinstance(artifacts, list):
        for artifact in artifacts:
            rows.append(Row(
                artifact_type=artifact_type,
                artifact_ddl=artifact,
                processed_at=current_timestamp()
            ))

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").saveAsTable("translation_results")
```

## Configuration

### Environment Variables

Set in cluster environment variables or job configuration:

```bash
# Required: Databricks LLM endpoint
DBX_ENDPOINT=databricks-llama-4-maverick

# Optional configuration
DDL_BATCH_SIZE=10
DDL_MAX_CONCURRENT=5
DDL_TIMEOUT=300
DDL_OUTPUT_DIR=/dbfs/FileStore/results
```

### Required Libraries

Add to cluster:
- `langchain>=0.1.0`
- `langchain-core>=0.1.0`
- `langchain-community>=0.1.0`
- `databricks-langchain>=0.1.0`
- `python-dotenv>=1.0.0`

### Cluster Configuration

**Recommended settings:**
- **Runtime:** Databricks Runtime 13.3 LTS or later
- **Node Type:** Standard (i3.xlarge or larger for large files)
- **Workers:** 2-4 workers (for parallel processing)

## File Path Formats

- **DBFS:** `dbfs:/FileStore/data/tables.json` (automatically converted to `/dbfs/FileStore/data/tables.json`)
- **Volumes:** `/Volumes/catalog/schema/volume_name/tables.json` (direct access)
- **Local:** `/tmp/tables.json` (used as-is)

### Reading from Different Sources

**From DBFS:**
```python
input_files = ["dbfs:/FileStore/data/tables.json"]
```

**From Volume:**
```python
input_files = ["/Volumes/main/default/migration_data/tables.json"]
```

**From External Location:**
```python
# First copy to DBFS or Volume
dbutils.fs.cp("s3://bucket/data/tables.json", "dbfs:/FileStore/data/tables.json")
input_files = ["dbfs:/FileStore/data/tables.json"]
```

## Complete Workflow Example

### Step 1: Prepare Input Files

```python
# Upload JSON files to DBFS or Volume
dbutils.fs.cp(
    "s3://your-bucket/snowflake_objects/tables.json",
    "dbfs:/FileStore/migration/tables.json"
)
```

### Step 2: Run Translation

```python
from translation_graph.databricks_job import process_translation_job

result = process_translation_job(
    input_files=[
        "dbfs:/FileStore/migration/tables.json",
        "dbfs:/FileStore/migration/views.json"
    ],
    output_path="dbfs:/FileStore/migration/results/translation_results.json",
    batch_size=10
)
```

### Step 3: Save Results to Delta Table

```python
import json
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# Read results
with open("/dbfs/FileStore/migration/results/translation_results.json", "r") as f:
    results = json.load(f)

# Convert to DataFrame
rows = []
for artifact_type, artifacts in results.items():
    if artifact_type != "metadata" and isinstance(artifacts, list):
        for artifact in artifacts:
            rows.append(Row(
                artifact_type=artifact_type,
                artifact_ddl=artifact,
                processed_at=current_timestamp()
            ))

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").saveAsTable("translation_results")
```

### Step 4: Execute Translated DDL (Optional)

```python
# Execute translated DDL statements
for artifact_type, artifacts in result.items():
    if artifact_type != "metadata" and isinstance(artifacts, list):
        for ddl in artifacts:
            try:
                spark.sql(ddl)
                print(f"✓ Executed {artifact_type} DDL")
            except Exception as e:
                print(f"✗ Error executing {artifact_type}: {str(e)}")
```

## Troubleshooting

### Common Issues

1. **Import errors:**
   ```python
   # Make sure path is correct
   import sys
   sys.path.insert(0, '/dbfs/FileStore/translation_graph')
   ```

2. **File not found:**
   - Verify path format: `dbfs:/` vs `/dbfs/`
   - Check file permissions
   - Ensure Volume access is granted

3. **LLM endpoint errors:**
   ```python
   # Set environment variable
   import os
   os.environ['DBX_ENDPOINT'] = 'your-endpoint-name'
   ```

4. **Memory issues:**
   - Reduce `batch_size`
   - Use larger cluster nodes
   - Process files sequentially

## Best Practices

1. **Use Volumes** for production data (better performance and access control)
2. **Set appropriate batch sizes** based on artifact complexity
3. **Monitor job logs** for translation errors
4. **Save intermediate results** for large migrations
5. **Use Delta tables** for storing translation results
6. **Implement retry logic** for transient failures
7. **Use job parameters** for flexible configuration

## Unity Catalog Configuration

If using Unity Catalog Volumes:

```sql
-- Grant access to Volume
GRANT READ VOLUME ON VOLUME main.default.migration_data TO `your_user@example.com`
```

```python
# Use in code
volume_path = "/Volumes/main/default/migration_data/"
```

