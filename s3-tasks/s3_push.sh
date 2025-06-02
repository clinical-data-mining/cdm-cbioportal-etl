#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

SCRIPTS_PATH=$1
CLUSTER_NAME=$2
BUCKET_NAME=$3
INPUT_DIR=$4
OUTPUT_DIR=$5
CREDS_FILE=$6

test -n "$SCRIPTS_PATH"
test -n "$CLUSTER_NAME"
test -n "$BUCKET_NAME"
test -n "$INPUT_DIR"
test -n "$OUTPUT_DIR"
test -n "$CREDS_FILE"

$SCRIPTS_PATH/authenticate_service_account.sh $CLUSTER_NAME $CREDS_FILE
aws s3 sync $INPUT_DIR s3://$BUCKET_NAME/$OUTPUT_DIR --profile saml