#!/usr/bin/env bash


set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/pipeline/summary/wrapper_cbioportal_summary_creator_testing_study.py"

# Run script
python $SCRIPT

