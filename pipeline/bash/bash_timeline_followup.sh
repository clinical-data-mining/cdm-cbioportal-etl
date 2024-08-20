#!/usr/bin/env bash

# Define which variables to use from msk_cdm.data_classes.<class> library
#VAR_SCRIPT="config_cbio_etl.script_create_summary_templates"


set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/pipeline/timeline/cbioportal_timeline_follow_up.py"

# Run script
python $SCRIPT

