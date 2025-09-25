#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
MINIO_ENV=$5
SAMPLE_LIST=$6
PATH_DATAHUB=$7
PROD_OR_TEST=$8

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$MINIO_ENV"
test -n "$SAMPLE_LIST"
test -n "$PATH_DATAHUB"
test -n "$PROD_OR_TEST"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../timeline

# Get variables
SCRIPT="cbioportal_timeline_deid.py"

echo $SCRIPT
echo $YAML_CONFIG

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --minio_env=$MINIO_ENV --cbio_sample_list=$SAMPLE_LIST --path_datahub=$PATH_DATAHUB --production_or_test=$PROD_OR_TEST
