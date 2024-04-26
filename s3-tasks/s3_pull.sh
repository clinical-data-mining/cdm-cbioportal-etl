/gpfs/mindphidata/cdm_repos/github/cdm-cbioportal-etl/s3-tasks/authenticate_service_account.sh private
aws s3 cp s3://cdm-deliverable/data_clinical_sample.txt $OUTPUT_DIR --profile saml
if [ $? -ne 0 ] ; then
  echo "`date`: Failed to download MSK_SOLID_HEME sample list from S3, exiting..."
fi