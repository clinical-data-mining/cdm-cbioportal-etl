#!/usr/bin/env bash

set -e

# Activate virtual env
source /gpfs/mindphidata/fongc2/miniconda3/etc/profile.d/conda.sh
conda activate conda-env-cdm

# Get variables
SCRIPT="/gpfs/mindphidata/fongc2/github/cdm-cbioportal-etl/pipeline/utils/save_anchor_dates.py"

# Run script
python $SCRIPT

