#!/usr/bin/env bash

# Generic bash script to execute timeline deidentification scripts
# This script can be used for medications, labs, diagnoses, or any other timeline data domain

set -e

ROOT_PATH_REPO=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
FNAME_DBX=$4
PATH_SCRIPT=$5
shift 5  # Remove first 5 args, remaining args are passed to Python script

test -n "$ROOT_PATH_REPO"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$FNAME_DBX"
test -n "$PATH_SCRIPT"

echo "================================================================================"
echo "TIMELINE DEIDENTIFICATION - Generic Executor"
echo "================================================================================"
echo "Repository path: $ROOT_PATH_REPO"
echo "Conda path: $CONDA_INSTALL_PATH"
echo "Conda env: $CONDA_ENV_NAME"
echo "Databricks env: $FNAME_DBX"
echo "Script to execute: $PATH_SCRIPT"
echo "Additional arguments: $@"
echo "================================================================================"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

# Change to repo directory
cd "$ROOT_PATH_REPO"

# Build full script path
SCRIPT_FULL_PATH="$ROOT_PATH_REPO/$PATH_SCRIPT"

echo "Full script path: $SCRIPT_FULL_PATH"

# Check if script exists
if [ ! -f "$SCRIPT_FULL_PATH" ]; then
    echo "ERROR: Script not found at $SCRIPT_FULL_PATH"
    exit 1
fi

echo "Executing script..."
echo ""

# Run script with fname_dbx and any additional arguments
python "$SCRIPT_FULL_PATH" --fname_dbx="$FNAME_DBX" "$@"

echo ""
echo "================================================================================"
echo "Script execution complete"
echo "================================================================================"
