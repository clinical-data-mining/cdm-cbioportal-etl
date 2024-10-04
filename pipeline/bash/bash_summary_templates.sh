#!/usr/bin/env bash

#REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../utils


# Get variables
SCRIPT="generate_cbioportal_template.py"
#YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"
# YAML_CONFIG="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/config/etl_config_mskarcher.yml"
echo "YAML CONFIG: "
echo $YAML_CONFIG

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

