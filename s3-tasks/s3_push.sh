#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$SCRIPTS_PATH"
test -n "$BUCKET_NAME"
test -n "$INPUT_DIR"
test -n "$OUTPUT_DIR"

$SCRIPTS_PATH/authenticate_service_account.sh $CLUSTER_NAME
aws s3 sync $INPUT_DIR s3://$BUCKET_NAME/$OUTPUT_DIR --profile saml