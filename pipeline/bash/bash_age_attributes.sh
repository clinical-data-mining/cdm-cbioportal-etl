#!/usr/bin/env bash

set -e

YAML_CONFIG=$1
test -n "$YAML_CONFIG"

CONDA_ENV_NAME="cdm-cbioportal-etl"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT="patient_age_info.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

