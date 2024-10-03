#!/usr/bin/env bash

#REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"


set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="${REPO_LOCATION}pipeline/utils/wrapper_cbioportal_copy_to_databricks.py"
YAML_CONFIG="${REPO_LOCATION}config/config_databricks_copy.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

