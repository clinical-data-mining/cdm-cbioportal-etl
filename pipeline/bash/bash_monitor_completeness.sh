#!/usr/bin/env bash

# Bash wrapper for cBioPortal data completeness monitoring
# This script checks that all data_clinical_*.txt and data_timeline_*.txt files
# have no completely empty columns

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
PATH_DATAHUB=$4

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$PATH_DATAHUB"

echo "================================================================================"
echo "CBIOPORTAL DATA COMPLETENESS MONITORING"
echo "================================================================================"
echo "Repository path: $REPO_LOCATION"
echo "Conda path: $CONDA_INSTALL_PATH"
echo "Conda env: $CONDA_ENV_NAME"
echo "Data path: $PATH_DATAHUB"
echo "================================================================================"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../monitoring

# Get variables
SCRIPT="monitoring_completeness.py"

echo "Running monitoring script..."
echo ""

# Run script
python $SCRIPT --path_datahub="$PATH_DATAHUB"

EXIT_CODE=$?

echo ""
echo "================================================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Data completeness monitoring PASSED"
else
    echo "Data completeness monitoring FAILED (exit code: $EXIT_CODE)"
fi
echo "================================================================================"

exit $EXIT_CODE
