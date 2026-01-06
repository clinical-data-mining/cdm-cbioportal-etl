#!/usr/bin/env bash

# Batch bash script to execute all timeline deidentification files in a loop
# This script calls the YAML-based batch Python wrapper which processes all timeline files

set -e

ROOT_PATH_REPO=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
CONFIG_DIR=$4
PRODUCTION_OR_TEST=$5
FNAME_DBX=$6
ANCHOR_DATES=$7
FNAME_SAMPLE=$8
VOLUME_BASE_PATH=$9
GPFS_OUTPUT_PATH=${10}
COHORT_NAME=${11}

test -n "$ROOT_PATH_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$CONFIG_DIR"
test -n "$PRODUCTION_OR_TEST"
test -n "$FNAME_DBX"
test -n "$ANCHOR_DATES"
test -n "$FNAME_SAMPLE"
test -n "$VOLUME_BASE_PATH"
test -n "$GPFS_OUTPUT_PATH"
test -n "$COHORT_NAME"

echo "================================================================================"
echo "TIMELINE BATCH DEIDENTIFICATION - Executor (YAML-based)"
echo "================================================================================"
echo "Repository path: $ROOT_PATH_REPO"
echo "Conda path: $CONDA_INSTALL_PATH"
echo "Conda env: $CONDA_ENV_NAME"
echo "Config directory: $CONFIG_DIR"
echo "Production/Test: $PRODUCTION_OR_TEST"
echo "Databricks env: $FNAME_DBX"
echo "Anchor dates: $ANCHOR_DATES"
echo "Sample list: $FNAME_SAMPLE"
echo "Volume base path: $VOLUME_BASE_PATH"
echo "GPFS output path: $GPFS_OUTPUT_PATH"
echo "Cohort name: $COHORT_NAME"
echo "================================================================================"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

# Change to repo directory
cd "$ROOT_PATH_REPO"

# Build full script path
SCRIPT_PATH="cdm-cbioportal-etl/pipeline/timeline/cbioportal_timeline_batch_deidentify.py"
SCRIPT_FULL_PATH="$ROOT_PATH_REPO/$SCRIPT_PATH"

echo "Script path: $SCRIPT_FULL_PATH"

# Check if script exists
if [ ! -f "$SCRIPT_FULL_PATH" ]; then
    echo "ERROR: Script not found at $SCRIPT_FULL_PATH"
    exit 1
fi

echo "Executing batch timeline deidentification..."
echo ""

# Run the batch deidentification script (YAML-based)
python "$SCRIPT_FULL_PATH" \
    --config_dir="$CONFIG_DIR" \
    --production_or_test="$PRODUCTION_OR_TEST" \
    --fname_dbx="$FNAME_DBX" \
    --anchor_dates="$ANCHOR_DATES" \
    --fname_sample="$FNAME_SAMPLE" \
    --volume_base_path="$VOLUME_BASE_PATH" \
    --gpfs_output_path="$GPFS_OUTPUT_PATH" \
    --cohort_name="$COHORT_NAME"

EXIT_CODE=$?

echo ""
echo "================================================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Batch timeline deidentification complete - SUCCESS"
else
    echo "Batch timeline deidentification complete - FAILED (exit code: $EXIT_CODE)"
fi
echo "================================================================================"

exit $EXIT_CODE
