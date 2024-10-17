#!/usr/bin/env bash

REPO_LOCATION="/gpfs/mindphidata/fongc2/github/"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/utils/generate_cbioportal_template.py"
YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact_testing_study.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

