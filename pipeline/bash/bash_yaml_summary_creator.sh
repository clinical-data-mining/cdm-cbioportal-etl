#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
DATABRICKS_ENV=$5
PATH_DATAHUB=$6
PROD_OR_TEST=$7
SAVE_TO_TABLE=${8:-""}  # Optional: --save-to-table flag

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$DATABRICKS_ENV"
test -n "$PATH_DATAHUB"
test -n "$PROD_OR_TEST"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

# Run YAML-based summary creator
SCRIPT=wrapper_yaml_summary_creator.py

if [ "$SAVE_TO_TABLE" == "--save-to-table" ]; then
    echo "Saving data and header files to Databricks tables"
    python $SCRIPT \
        --config_yaml=$YAML_CONFIG \
        --databricks_env=$DATABRICKS_ENV \
        --path_datahub=$PATH_DATAHUB \
        --production_or_test=$PROD_OR_TEST \
        --save_to_table
else
    echo "Saving data and header files to volume only"
    python $SCRIPT \
        --config_yaml=$YAML_CONFIG \
        --databricks_env=$DATABRICKS_ENV \
        --path_datahub=$PATH_DATAHUB \
        --production_or_test=$PROD_OR_TEST
fi
