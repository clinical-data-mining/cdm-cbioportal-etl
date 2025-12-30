#!/usr/bin/env bash

# Bash script to execute timeline audit and generate summary report
# Analyzes timeline files in Databricks volumes and saves summary to Databricks

set -e

ROOT_PATH_REPO=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
FNAME_DBX=$4
COHORT_NAME=$5
REFERENCE_FILE=$6
VOLUME_BASE_PATH=$7
OUTPUT_VOLUME_PATH=$8

test -n "$ROOT_PATH_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$FNAME_DBX"
test -n "$COHORT_NAME"
test -n "$REFERENCE_FILE"
test -n "$VOLUME_BASE_PATH"
test -n "$OUTPUT_VOLUME_PATH"

echo "================================================================================"
echo "TIMELINE AUDIT - Executor"
echo "================================================================================"
echo "Repository path: $ROOT_PATH_REPO"
echo "Conda path: $CONDA_INSTALL_PATH"
echo "Conda env: $CONDA_ENV_NAME"
echo "Databricks env: $FNAME_DBX"
echo "Cohort name: $COHORT_NAME"
echo "Reference file: $REFERENCE_FILE"
echo "Volume base path: $VOLUME_BASE_PATH"
echo "Output volume path: $OUTPUT_VOLUME_PATH"
echo "================================================================================"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

# Change to repo directory
cd "$ROOT_PATH_REPO"

# Build full script path
SCRIPT_PATH="cdm-cbioportal-etl/pipeline/monitoring/cbioportal_timeline_audit.py"
SCRIPT_FULL_PATH="$ROOT_PATH_REPO/$SCRIPT_PATH"

echo "Script path: $SCRIPT_FULL_PATH"

# Check if script exists
if [ ! -f "$SCRIPT_FULL_PATH" ]; then
    echo "ERROR: Script not found at $SCRIPT_FULL_PATH"
    exit 1
fi

echo "Executing timeline audit..."
echo ""

# Run the audit script
python "$SCRIPT_FULL_PATH" \
    --fname_dbx="$FNAME_DBX" \
    --cohort_name="$COHORT_NAME" \
    --reference_file="$REFERENCE_FILE" \
    --volume_base_path="$VOLUME_BASE_PATH" \
    --output_volume_path="$OUTPUT_VOLUME_PATH" \
    --create_table

EXIT_CODE=$?

echo ""
echo "================================================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Timeline audit complete - SUCCESS"
else
    echo "Timeline audit complete - FAILED (exit code: $EXIT_CODE)"
fi
echo "================================================================================"

exit $EXIT_CODE