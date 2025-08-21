#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
MINIO_ENV=$5
PATH_DATAHUB=$6

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$MINIO_ENV"
test -n "$PATH_DATAHUB"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT=wrapper_cbioportal_summary_creator.py

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --minio_env=$MINIO_ENV --path_datahub=$PATH_DATAHUB
