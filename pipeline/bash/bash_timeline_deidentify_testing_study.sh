#!/usr/bin/env bash

set -e

CONDA_INSTALL_PATH=/gpfs/mindphidata/fongc2/miniconda3
CONDA_ENV_NAME=conda-env-cdm-fongc2
YAML_CONFIG=/gpfs/mindphidata/fongc2/github/cdm-cbioportal-etl/config/etl_config_mskarcher.yml
MINIO_ENV=/gpfs/mindphidata/fongc2/minio_env_dev.txt
SAMPLE_LIST=/gpfs/mindphidata/cdm_repos_dev/dev/data/impact-data/mskarcher/data_clinical_sample.txt
PATH_DATAHUB=/gpfs/mindphidata/cdm_repos_dev/dev/data/cdm-data/mskarcher
TEST=test

test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$MINIO_ENV"
test -n "$SAMPLE_LIST"
test -n "$PATH_DATAHUB"

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
python $SCRIPT --config_yaml=$YAML_CONFIG --minio_env=$MINIO_ENV --cbio_sample_list=$SAMPLE_LIST --path_datahub=$PATH_DATAHUB --production_or_test=$TEST
