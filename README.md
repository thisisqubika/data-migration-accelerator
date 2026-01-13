# Data Migration Accelerator

A project for testing and validating Snowflake to Databricks migration tools and extractors.

## Overview

This project contains Snowflake test objects that can be used to validate extractors and conversion tools that migrate Snowflake database objects to Databricks.

## Files

- **snowflake_test_objects.sql** - Contains sample Snowflake objects (tables, views, procedures, functions, etc.) with the `data_migration` naming convention for testing migration tools.
- **snowpark.py** - Python script using Snowpark API to read and extract all Snowflake objects from the database.
- **CONFIGURATION.md** - Detailed guide on configuring Snowflake credentials.

## Usage

1. Execute `snowflake_test_objects.sql` in your Snowflake environment to create the test objects
2. Use your migration tool/extractor to convert these Snowflake objects to Databricks
3. Validate the converted objects in Databricks

### Reading Snowflake Objects with Python

To read all Snowflake objects programmatically using Snowpark:

#### Setup

1. **Set up virtual environment (recommended):**
   ```bash
   # Create virtual environment
   python3 -m venv venv
   
   # Activate virtual environment
   # On macOS/Linux:
   source venv/bin/activate
   # On Windows:
   # venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```
   
   **Note:** The virtual environment has already been created and dependencies installed. To activate it:
   ```bash
   source venv/bin/activate
   ```

2. **Configure credentials:**
   - Copy the example environment file:
     ```bash
     cp env.example .env
     ```
   - Edit `.env` with your Snowflake credentials (see [CONFIGURATION.md](CONFIGURATION.md) for details):
     - Required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
     - Optional: `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_REGION`

#### Using snowpark.py

```bash
# Make sure virtual environment is activated
source venv/bin/activate  # On macOS/Linux

# Run the script
python snowpark.py
```

**Features:**
- Uses Snowpark API for data processing
- Uses password authentication
- Reads all objects (tables, views, procedures, functions, sequences, stages, file formats, tasks, streams, pipes)
- Includes sample data from tables (first 10 rows)
- Queries specific test objects from `snowflake_test_objects.sql`
- Displays a summary
- Saves results to `snowflake_objects_snowpark.json`

## Test Objects

The SQL file includes:

- **Tables**: `data_migration_source`, `data_migration_target`
- **Views**: Various views for data migration summaries and status
- **Stored Procedures**: Procedures for querying migration data
- **User-Defined Functions**: Scalar and table functions
- **Other Objects**: Sequences, stages, file formats, tasks, streams, and pipes

## Configuration

- **Database**: `DATA_MIGRATION_DB`
- **Schema**: `DATA_MIGRATION_SCHEMA`
- **Data Retention**: 1 day (max retention)

### Credentials Configuration

For detailed instructions on configuring Snowflake credentials, see [CONFIGURATION.md](CONFIGURATION.md).

**Quick Setup:**
1. Copy the example environment file:
   ```bash
   cp env.example .env
   ```

2. Edit `.env` with your credentials:
   ```env
   SNOWFLAKE_ACCOUNT=your_account_identifier
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_DATABASE=your_database    # Required
   SNOWFLAKE_SCHEMA=your_schema        # Required
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH      # Optional
   SNOWFLAKE_ROLE=SYSADMIN             # Optional
   
   # Unity Catalog - Required
   UC_CATALOG=your_catalog_name
   UC_SCHEMA=migration_accelerator
   ```

3. The `.env` file is already in `.gitignore` to protect your credentials

### Databricks Deployment

To run on Databricks, configure the following:

#### Databricks Secrets

Create a secrets scope and add credentials:

```bash
databricks secrets create-scope migration-accelerator
databricks secrets put-secret migration-accelerator SNOWFLAKE_ACCOUNT
databricks secrets put-secret migration-accelerator SNOWFLAKE_USER
databricks secrets put-secret migration-accelerator SNOWFLAKE_PASSWORD
databricks secrets put-secret migration-accelerator DATABRICKS_HOST
databricks secrets put-secret migration-accelerator DATABRICKS_CLIENT_ID
databricks secrets put-secret migration-accelerator DATABRICKS_CLIENT_SECRET
```

#### Cluster Environment Variables

Set in **Cluster → Advanced Options → Spark → Environment Variables**:

```bash
UC_CATALOG=your_catalog_name
UC_SCHEMA=migration_accelerator
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

#### GitHub Secrets (for CI/CD)

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_CLIENT_ID` | OAuth M2M client ID |
| `DATABRICKS_CLIENT_SECRET` | OAuth M2M client secret |
| `DATABRICKS_CLUSTER_ID` | Cluster ID for jobs |
| `UC_CATALOG` | Unity Catalog name |
| `DEVS_GROUP` | Group name for permissions |

> **Note:** The `DEVS_GROUP` (e.g., `migration-accelerator-devs`) must exist in Databricks before deployment. Create it in **Admin Console → Groups → Create Group**.

#### After deployment

Once deployed, get the service principal name from the Databricks App in Compute->Apps->dbx-job-executor-app->Authorization->App Authorization and th job id from Jobs & Pipelines->snowflake_ingestion_job->Job Details->Job ID. Then add this Service Principal to the developers permission group specified in the variable DEVS_GROUP in the Github Secrets. 

#### Handle Results

The results are stored in /Volumes/<databricks_host>/migration_accelerator/outputs/<timestamp>, these are the SQL files that will create the databricks artifacts once ran.

The recommended order of SQL files to run is:
Roles → Stages → Tables → Streams → Pipes → Views → UDFs → Procedures → Grants

## Run Locally (translation job)


For a stylish, repeatable local run, use the included helper script or Makefile target.

Quick (recommended):

```bash
# run via make (creates timestamped output dir)
make translate
```

Direct script (more control):

```bash
# run all example input files, batch size 2, produce SQL files
./scripts/run_translation.sh

# pass a custom glob, batch size, or output format (json/sql)
./scripts/run_translation.sh "src/artifact_translation_package/examples/*.json" 4 json
```

Notes:
- `make translate` invokes the script at `scripts/run_translation.sh` and is the cleanest option.
- The script exports `PYTHONPATH=src` so you can run it from the repository root.
- Output is written to `src/artifact_translation_package/out_sql_examples_<timestamp>`.

Local vs Databricks output paths
--------------------------------

This project uses Databricks-style paths (for example `dbfs:/...`) as the canonical configuration. To make the same code run locally without changing the canonical config, the runner maps Databricks paths to a local directory when it detects a non-Databricks runtime.

- **Local mapping environment variable**: set `LOCAL_DBFS_MOUNT` to the local directory that should act as the root for `dbfs:/` paths. Default: `./ddl_output`.
- **Per-run `results_dir`**: when running the translation job locally (for example via `make translate`), the job pre-creates a timestamped `results_dir` and propagates it to the translation context. Evaluation results are written under `<results_dir>/evaluation_results/`. Translation outputs such as `translation_results.json` and `results_summary.json` are written into the same `results_dir`.
- **Fallback behavior**: the runner also exports `DDL_OUTPUT_DIR=<results_dir>` so code paths that read the `DDL_OUTPUT_DIR` environment variable will also target the per-run folder.

Examples

```bash
# run with default local mapping (output under ./ddl_output by default)
make translate

# override where dbfs:/ maps to locally
export LOCAL_DBFS_MOUNT=/tmp/my_local_dbfs
make translate

# inspect latest run outputs
ls -la src/artifact_translation_package/out_sql_examples_*/
ls -la <that-run-folder>/evaluation_results/
```
