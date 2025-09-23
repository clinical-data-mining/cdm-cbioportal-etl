#!/usr/bin/env bash

set -e

CONDA_INSTALL_PATH=/gpfs/mindphidata/fongc2/miniconda3
CONDA_ENV_NAME=conda-env-cdm-fongc2
YAML_CONFIG=/gpfs/mindphidata/fongc2/github/cdm-cbioportal-etl/config/etl_config_mskimpact_heme.yml
MINIO_ENV=/gpfs/mindphidata/fongc2/minio_env_dev.txt
PATH_DATAHUB=/gpfs/mindphidata/cdm_repos_dev/dev/data/cdm-data/mskimpact_heme
TEST=test

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
python $SCRIPT --config_yaml=$YAML_CONFIG --minio_env=$MINIO_ENV --path_datahub=$PATH_DATAHUB  --production_or_test=$TEST
