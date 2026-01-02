# SQL Translation Model Benchmark Evaluation

Evaluate and compare SQL translation models (Snowflake → Databricks) using MLflow and LLM-as-a-judge.

## Prerequisites

Before running the benchmark, ensure you have:

1. **Databricks CLI configured** (or `.env` file with credentials):
   ```bash
   # Option A: Databricks CLI
   databricks configure --host https://your-workspace.cloud.databricks.com
   
   # Option B: .env file in project root
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=dapi...
   ```

2. **Dependencies installed**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Input Data**: The benchmark expects input JSON files (Snowflake DDL metadata). By default, it looks in `src/artifact_translation_package/examples/`.

## Quick Start (Running the Evaluation)

### Option 1: Interactive Notebook
The simplest way to run and visualize results.

1. Open `src/artifact_translation_package/evaluation/benchmark_interactive.ipynb`
2. **Configure models**: Set your endpoints in the Config cell.
   ```python
   TRANSLATION_MODELS = ["databricks-llama-4-maverick", "databricks-gemini-2-5-flash"]
   ARTIFACT_TYPE = "tables"  # or "views", "procedures"
   ```
3. **Run all cells**: It will trigger `run_local_benchmark.py` and display comparison charts.

### Option 2: Command Line (Fastest)

Run the benchmark from the project root:

```bash
# 1. Basic: Run benchmark for tables using default models
python3 run_local_benchmark.py --artifact-type tables

# 2. Advanced: Specify custom models to compare
python3 run_local_benchmark.py \
  --artifact-type views \
  --models databricks-llama-4-maverick databricks-meta-llama-3-1-70b-instruct

# 3. Custom Data: Specify a custom input JSON file
python3 run_local_benchmark.py \
  --artifact-type tables \
  --dataset-source /path/to/your/metadata.json

# 4. Settings: Control batch size for judge execution
python3 run_local_benchmark.py --batch-size 10
```

## How Evaluation Works

We use a **Strict Deduction-Based Scoring System** (starting at 100) to evaluate two independent dimensions:

### Dimension 1: Compliance Score (0-100)
**Goal**: Functional correctness. Can this code actually run on Databricks?
- **Invalid syntax**: Automatic score of **0**.
- **Point Deductions**:
  - Missing `USING DELTA` (-20 pts)
  - Using legacy types like `VARCHAR` or `TEXT` instead of `STRING` (-10 pts)
  - Missing 3-level naming (`catalog.schema.table`) (-15 pts)

### Dimension 2: Best Practices Score (0-100)
**Goal**: Performance and Documentation. Is this production-grade code?
- **Point Deductions**:
  - Missing `CLUSTER BY` (Liquid Clustering) (-30 pts)
  - Missing table properties like `autoOptimize` (-20 pts)
  - Missing table or column `COMMENT`s (-25 pts each)

## MLflow Features

The benchmark automatically logs rich data to Databricks MLflow:

- **Experiment Name**: Defaults to `sql-translation-benchmark` or your username.
- **Searchable Tags**: Every run is tagged with issue categories (e.g., `has_naming_issues: true`).
- **Issues Table**: `issues_table.json` logs every single violation found for queryable analysis.
- **Top Issues Summary**: `top_issues_summary.txt` provides an at-a-glance summary of the most common mistakes across all samples.

## Metrics Reference

| Metric | Threshold | Description |
|--------|-----------|-------------|
| `avg_compliance` | 0-100 | Mean functional correctness score. |
| `avg_best_practices` | 0-100 | Mean optimization/docs score. |
| `compliant_pct` | >= 70 | % of statements that are functional. |
| `syntax_valid_pct` | 100% | % of statements with valid Databricks SQL syntax. |

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| Authentication Error | Missing `DATABRICKS_TOKEN` | Check `.env` or run `databricks configure`. |
| `File Not Found` | Custom JSON path is wrong | Verify `--dataset-source` path. |
| Model not found | Incorrect endpoint name | Verify name in Databricks Model Serving UI. |
| Low scores | Model performance | Check `top_issues_summary.txt` to find systemic errors. |

## Deployment to Databricks

To run this directly within a Databricks Job or Notebook:
1. Ensure the `requirements.txt` libraries are installed on the cluster.
2. The benchmark will automatically detect it is running in Databricks and create MLflow experiments in your User workspace folder.
