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
cd ../timeline

# Get variables
SCRIPT="cbioportal_timeline_deid.py"

echo $SCRIPT
echo $YAML_CONFIG

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG