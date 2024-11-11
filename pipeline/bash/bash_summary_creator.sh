#!/usr/bin/env bash

#REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"
YAML_CONFIG="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/config/etl_config_mskimpact.yml"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

# Get variables
#SCRIPT=$(python -c "from msk_cdm.data_classes.legacy import CDMProcessingVariablesCbioportal as config_cbio_etl; print (${VAR_SCRIPT})")
SCRIPT=wrapper_cbioportal_summary_creator.py
#YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_mskimpact.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG

