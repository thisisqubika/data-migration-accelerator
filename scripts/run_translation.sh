#!/usr/bin/env bash
set -euo pipefail

# Stylish helper to run the translation job locally and produce timestamped SQL output
# Usage: ./scripts/run_translation.sh [--examples "path/glob"] [--batch-size N] [--format sql|json]

EXAMPLES_GLOB=${1:-src/artifact_translation_package/examples/*.json}
BATCH_SIZE=${2:-2}
FORMAT=${3:-sql}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
# Prefer an explicit DDL_OUTPUT_DIR if provided (e.g. from .env). Otherwise
# fall back to creating a timestamped out_sql_examples_<timestamp> folder.
OUT_DIR="${DDL_OUTPUT_DIR:-src/artifact_translation_package/out_sql_examples_${TIMESTAMP}}"

echo "-> Running translation job"
echo "   examples: ${EXAMPLES_GLOB}"
echo "   batch size: ${BATCH_SIZE}"
echo "   output format: ${FORMAT}"
echo "   output dir: ${OUT_DIR} (DDL_OUTPUT_DIR=${DDL_OUTPUT_DIR:-unset})"

mkdir -p "$OUT_DIR"

export PYTHONPATH=src
python3 -m artifact_translation_package.databricks_job \
  --input-files ${EXAMPLES_GLOB} \
  --output-path "$OUT_DIR" \
  --batch-size ${BATCH_SIZE} \
  --output-format ${FORMAT}

echo "\n=> SQL files saved to: $OUT_DIR"
ls -la "$OUT_DIR"
