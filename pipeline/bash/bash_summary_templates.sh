#!/usr/bin/env bash

#REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
echo "$MY_PATH"
cd $MY_PATH
cd ../util


# Get variables
SCRIPT="generate_cbioportal_template.py"
#YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

