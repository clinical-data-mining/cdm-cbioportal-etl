#!/usr/bin/env bash

# Bash wrapper for modular summary pipeline
# This script calls the YAML-based summary pipeline which processes patient and sample summaries

set -e

ROOT_PATH_REPO=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
CONFIG_DIR=$4
DATABRICKS_ENV=$5
ANCHOR_DATES=$6
TEMPLATE_PATIENT=$7
TEMPLATE_SAMPLE=$8
OUTPUT_DIR_DATABRICKS=$9
OUTPUT_DIR_LOCAL=${10}
CATALOG=${11}
SCHEMA=${12}
PRODUCTION_OR_TEST=${13}
COHORT=${14}
PROCESS_PATIENT=${15:-"--patient"}  # Default to processing patient
PROCESS_SAMPLE=${16:-"--sample"}    # Default to processing sample

test -n "$ROOT_PATH_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$CONFIG_DIR"
test -n "$DATABRICKS_ENV"
test -n "$ANCHOR_DATES"
test -n "$TEMPLATE_PATIENT"
test -n "$TEMPLATE_SAMPLE"
test -n "$OUTPUT_DIR_DATABRICKS"
test -n "$OUTPUT_DIR_LOCAL"
test -n "$CATALOG"
test -n "$SCHEMA"
test -n "$PRODUCTION_OR_TEST"
test -n "$COHORT"

echo "================================================================================"
echo "MODULAR SUMMARY PIPELINE - Executor (YAML-based)"
echo "================================================================================"
echo "Repository path: $ROOT_PATH_REPO"
echo "Conda path: $CONDA_INSTALL_PATH"
echo "Conda env: $CONDA_ENV_NAME"
echo "Config directory: $CONFIG_DIR"
echo "Databricks env: $DATABRICKS_ENV"
echo "Anchor dates: $ANCHOR_DATES"
echo "Template patient: $TEMPLATE_PATIENT"
echo "Template sample: $TEMPLATE_SAMPLE"
echo "Output dir (Databricks): $OUTPUT_DIR_DATABRICKS"
echo "Output dir (Local): $OUTPUT_DIR_LOCAL"
echo "Catalog: $CATALOG"
echo "Schema: $SCHEMA"
echo "Production/Test: $PRODUCTION_OR_TEST"
echo "Cohort: $COHORT"
echo "Process patient: $PROCESS_PATIENT"
echo "Process sample: $PROCESS_SAMPLE"
echo "================================================================================"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

# Change to repo directory
cd "$ROOT_PATH_REPO"

# Build full script path
SCRIPT_PATH="cdm-cbioportal-etl/pipeline/summary/wrapper_modular_summary_pipeline.py"
SCRIPT_FULL_PATH="$ROOT_PATH_REPO/$SCRIPT_PATH"

echo "Script path: $SCRIPT_FULL_PATH"

# Check if script exists
if [ ! -f "$SCRIPT_FULL_PATH" ]; then
    echo "ERROR: Script not found at $SCRIPT_FULL_PATH"
    exit 1
fi

echo "Executing modular summary pipeline..."
echo ""

# Build command with optional flags
CMD="python \"$SCRIPT_FULL_PATH\" \
    --config_dir=\"$CONFIG_DIR\" \
    --databricks_env=\"$DATABRICKS_ENV\" \
    --anchor_dates=\"$ANCHOR_DATES\" \
    --template_patient=\"$TEMPLATE_PATIENT\" \
    --template_sample=\"$TEMPLATE_SAMPLE\" \
    --output_dir_databricks=\"$OUTPUT_DIR_DATABRICKS\" \
    --output_dir_local=\"$OUTPUT_DIR_LOCAL\" \
    --catalog=\"$CATALOG\" \
    --schema=\"$SCHEMA\" \
    --production_or_test=\"$PRODUCTION_OR_TEST\" \
    --cohort=\"$COHORT\""

# Add optional patient/sample flags
if [ "$PROCESS_PATIENT" == "--patient" ]; then
    CMD="$CMD --patient"
fi

if [ "$PROCESS_SAMPLE" == "--sample" ]; then
    CMD="$CMD --sample"
fi

# Run the command
eval $CMD

EXIT_CODE=$?

echo ""
echo "================================================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Modular summary pipeline complete - SUCCESS"
else
    echo "Modular summary pipeline complete - FAILED (exit code: $EXIT_CODE)"
fi
echo "================================================================================"

exit $EXIT_CODE
