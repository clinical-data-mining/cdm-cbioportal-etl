#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

echo $PATH

/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/s3-tasks/authenticate_service_account_dev.sh private
aws s3 cp s3://cdm-deliverable/data_clinical_sample.txt {{ params.output_dir }} --profile saml