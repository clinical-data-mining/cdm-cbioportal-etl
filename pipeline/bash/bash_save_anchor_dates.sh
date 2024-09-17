#!/usr/bin/env bash

set -e

REPO_LOCATION="/gpfs/mindphidata/fongc2/github/"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm-fongc2

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/utils/save_anchor_dates.py"
YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

