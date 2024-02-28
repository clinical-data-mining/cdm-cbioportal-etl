# TODO iron out path?
/mind_data/cdm_repos/cdm-cbioportal-etl/s3-tasks/authenticate_service_account.sh eks
aws s3 sync $INPUT_DIR s3://cdm-deliverable/$OUTPUT_DIR --profile saml
if [ $? -ne 0 ] ; then
  echo "`date`: Failed to upload CDM annotated data to S3 bucket, exiting..."
fi