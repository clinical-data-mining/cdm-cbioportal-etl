#!/usr/bin/env bash

set -e

CONDA_ENV_NAME="cdm-cbioportal-etl"

YAML_CONFIG=$1
test -n "$YAML_CONFIG"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../timeline

# Get variables
SCRIPT="cbioportal_timeline_follow_up.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

