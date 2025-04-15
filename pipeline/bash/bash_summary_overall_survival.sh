#!/usr/bin/env bash

#REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"
#YAML_CONFIG="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/config/etl_config_mskimpact.yml"
CONDA_ENV_NAME="cdm-cbioportal-etl"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

SCRIPT="cbioportal_overall_survival.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

