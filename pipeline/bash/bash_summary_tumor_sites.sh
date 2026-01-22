#!/usr/bin/env bash
set -e

#REPO_LOCATION=/gpfs/mindphidata/cdm_repos/github/
#CONDA_INSTALL_PATH=/gpfs/mindphidata/fongc2/miniconda3
#CONDA_ENV_NAME=cdsi-nlp-inference
#DATABRICKS_ENV=/gpfs/mindphidata/fongc2/databricks_env_prod.txt
#TABLE_PATH="cdsi_eng_phi.cdm_eng_rad_tumor_sites.table_timeline_radiology_tumor_sites_predictions"
#OUTPUT_TABLE="cdsi_eng_phi.cdm_eng_rad_tumor_sites.table_summary_radiology_tumor_sites_predictions"
#VOLUME_PATH="/Volumes/cdsi_eng_phi/cdm_eng_rad_tumor_sites/cdm_eng_rad_tumor_sites_volume/table_summary_radiology_tumor_sites_predictions.tsv"

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
TABLE_PATH=$4
DATABRICKS_ENV=$5
OUTPUT_TABLE=$6
VOLUME_PATH=$7
#
#test -n "$REPO_LOCATION"
#test -n "$CONDA_INSTALL_PATH"
#test -n "$CONDA_ENV_NAME"
#test -n "$TABLE_PATH"
#test -n "$DATABRICKS_ENV"
#test -n "$OUTPUT_TABLE"
#test -n "$VOLUME_PATH"
#
## Activate virtual env
#source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
#conda activate "$CONDA_ENV_NAME"
#
#MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
#cd $MY_PATH
#cd ../cbioportal_formatting
#
## Get script name
#SCRIPT="cbioportal_formatting_radrpt_tumor_sites_summary.py"
#
## Run script
#python $SCRIPT \
#    --table_path=$TABLE_PATH \
#    --databricks_env=$DATABRICKS_ENV \
#    --output_table=$OUTPUT_TABLE \
#    --volume_path=$VOLUME_PATH


#!/usr/bin/env bash

set -e

REPO_LOCATION=$1
CONDA_INSTALL_PATH=$2
CONDA_ENV_NAME=$3
YAML_CONFIG=$4
DATABRICKS_ENV=$5

test -n "$REPO_LOCATION"
test -n "$CONDA_INSTALL_PATH"
test -n "$CONDA_ENV_NAME"
test -n "$YAML_CONFIG"
test -n "$DATABRICKS_ENV"

# Activate virtual env
source $CONDA_INSTALL_PATH/etc/profile.d/conda.sh
conda activate "$CONDA_ENV_NAME"

MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd $MY_PATH
cd ../summary

SCRIPT="cbioportal_summary_tumor_sites.py"

# Run script
python $SCRIPT --config_yaml=$YAML_CONFIG --databricks_env=$DATABRICKS_ENV
