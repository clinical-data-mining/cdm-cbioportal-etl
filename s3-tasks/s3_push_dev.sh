#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$INPUT_DIR"
test -n "$OUTPUT_DIR"

/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/s3-tasks/authenticate_service_account_dev.sh private
aws s3 sync $INPUT_DIR s3://cdm-deliverable/$OUTPUT_DIR --profile saml