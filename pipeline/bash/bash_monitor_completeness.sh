#!/usr/bin/env bash

REPO_LOCATION="/gpfs/mindphidata/fongc2/github/"
FNAME_LOG="/gpfs/mindphidata/fongc2/github/cdm-cbioportal-etl/logs/log_complete_cdm_cbioportal_etl_20240917.csv"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm-fongc2

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/monitoring/monitoring_completeness.py"
YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT \
  --config_yaml=$YAML_CONFIG \
  --fname_log="$FNAME_LOG"

