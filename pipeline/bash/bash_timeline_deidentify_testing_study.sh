#!/usr/bin/env bash

set -e

YAML_CONFIG="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/config/etl_config_all_impact_testing_study.yml"
CONDA_ENV_NAME="cdm-cbioportal-etl"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../timeline

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT=cbioportal_timeline_deid.py

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

