#!/usr/bin/env bash

# Define which variables to use from msk_cdm.data_classes.<class> library
# TODO make these inputs instead of hardcoded variables
VAR_SCRIPT="config_cbio_etl.script_create_summary_templates"
#ENV_CRED="/gpfs/mindphidata/fongc2/.env_cf"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get creds
#. /gpfs/mindphidata/fongc2/.env_cf

# Get variables
SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")

# Run script
python $SCRIPT

