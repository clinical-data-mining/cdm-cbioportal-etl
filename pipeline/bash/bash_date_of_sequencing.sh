#!/usr/bin/env bash

REPO_LOCATION="/gpfs/mindphidata/cdm_repos/github/"
FNAME_SAVE="cbioportal/seq_date.txt"

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="${REPO_LOCATION}cdm-cbioportal-etl/pipeline/utils/generate_date_of_sequencing.py"
YAML_CONFIG="${REPO_LOCATION}cdm-cbioportal-etl/config/etl_config_all_impact.yml"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --fname_save_date_of_seq=$FNAME_SAVE

