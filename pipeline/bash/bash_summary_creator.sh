#!/usr/bin/env bash

set -e

CONDA_ENV_NAME="cdm-cbioportal-etl"

YAML_CONFIG=$1

test -n "$YAML_CONFIG"

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
python $SCRIPT --config_yaml=$YAML_CONFIG

