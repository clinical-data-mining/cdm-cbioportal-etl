#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
FNAME_LOG=$5

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$FNAME_LOG"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../monitoring

# Get variables
SCRIPT="monitoring_completeness.py"
#YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT \
  --config_yaml=$YAML_CONFIG \
  --incomplete_fields_csv="$FNAME_LOG"
