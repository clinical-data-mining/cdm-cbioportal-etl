#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
YAML_CONFIG=$2
FNAME_LOG=$3

test -n "$REPO_LOCATION"
test -n "$YAML_CONFIG"
test -n "$FNAME_LOG"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/monitoring/monitoring_completeness.py"

# Run script
python $SCRIPT \
  --config_yaml=$YAML_CONFIG \
  --incomplete_fields_csv="$FNAME_LOG"