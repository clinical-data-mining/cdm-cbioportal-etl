#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
YAML_CONFIG=$2

test -n "$REPO_LOCATION"
test -n "$YAML_CONFIG"

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

# Get variables
SCRIPT=wrapper_cbioportal_summary_creator.py

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG