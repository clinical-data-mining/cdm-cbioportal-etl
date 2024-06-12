#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$BUCKET_NAME"
test -n "$BUCKET_KEY"
test -n "$OUTPUT_DIR"

/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/s3-tasks/authenticate_service_account.sh private
aws s3 cp s3://$BUCKET_NAME/$BUCKET_KEY $OUTPUT_DIR --profile saml

# Since the BUCKET_KEY is used as a sensor, we must remove it after downloading
aws s3 rm s3://$BUCKET_NAME/$BUCKET_KEY