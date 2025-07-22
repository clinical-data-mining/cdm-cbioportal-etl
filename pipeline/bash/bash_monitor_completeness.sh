#!/usr/bin/env bash

set -e

CONDA_ENV_NAME="cdm-cbioportal-etl"

YAML_CONFIG=$1
FNAME_LOG=$2

test -n "$YAML_CONFIG"
test -n "$FNAME_LOG"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../monitoring

# Get variables
SCRIPT="monitoring_completeness.py"
#YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT \
  --config_yaml=$YAML_CONFIG \
  --incomplete_fields_csv="$FNAME_LOG"

