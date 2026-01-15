#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
DATABRICKS_ENV=$5

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$DATABRICKS_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

SCRIPT="cbioportal_overall_survival.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --databricks_env=$DATABRICKS_ENV
