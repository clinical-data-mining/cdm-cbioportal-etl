#!/usr/bin/env bash

set -e

FNAME_SAVE="cbioportal/seq_date.txt"
CONDA_ENV_NAME="cdm-cbioportal-etl"

YAML_CONFIG=$1
test -n "$YAML_CONFIG"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../utils

# Get variables
SCRIPT="generate_date_of_sequencing.py"


# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --fname_save_date_of_seq=$FNAME_SAVE