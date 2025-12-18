#!/usr/bin/env bash

set -e

FNAME_SAVE="cbioportal/seq_date.txt"

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
DATABRICKS_ENV=$4

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$DATABRICKS_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../utils

# Get variables
SCRIPT="generate_date_of_sequencing.py"

# Run script
python $SCRIPT --fname_save_date_of_seq=$FNAME_SAVE --databricks_env=$DATABRICKS_ENV