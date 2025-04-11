#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
YAML_CONFIG=$2

test -n "$REPO_LOCATION"
test -n "$YAML_CONFIG"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/timeline/cbioportal_timeline_follow_up.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG