#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
TIMELINE_FOLDER=$4

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$TIMELINE_FOLDER"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate $CONDA_ENV_NAME

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../timeline

# Get variables
SCRIPT="cbioportal_timeline_truncate_to_fu.py"

# Run script
python $SCRIPT --timeline_folder=$TIMELINE_FOLDER
