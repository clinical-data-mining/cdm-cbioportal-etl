#!/usr/bin/env bash
set -e

REPO_LOCATION=/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl
CONDA_INSTALL_PATH=/gpfs/mindphidata/fongc2/miniconda3
CONDA_ENV_NAME=conda-env-cdm-fongc2
MINIO_ENV=/gpfs/mindphidata/fongc2/minio_env_dev.txt

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$MINIO_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../utils

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT="generate_age_at_sequencing.py"

# Run script
python $SCRIPT --minio_env=$MINIO_ENV
