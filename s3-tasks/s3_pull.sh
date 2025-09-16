#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

SCRIPTS_PATH=$1
CLUSTER_NAME=$2
BUCKET_NAME=$3
SAMPLE_FILEPATH=$4
OUTPUT_DIR=$5
CREDS_FILE=$6

test -n "$SCRIPTS_PATH"
test -n "$CLUSTER_NAME"
test -n "$BUCKET_NAME"
test -n "$SAMPLE_FILEPATH"
test -n "$OUTPUT_DIR"
test -n "$CREDS_FILE"

# Create directory if it doesn't exist
mkdir -p $OUTPUT_DIR

# Authenticate and download clinical sample file from S3
$SCRIPTS_PATH/authenticate_service_account.sh $CLUSTER_NAME $CREDS_FILE
aws s3 cp s3://$BUCKET_NAME/$SAMPLE_FILEPATH $OUTPUT_DIR --profile saml