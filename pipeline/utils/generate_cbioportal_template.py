import argparse
# import sys
# import os

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.summary import cbioportal_template_generator
from lib.utils import cbioportal_update_config
# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# from variables import (
#     ENV_MINIO,
#     PATH_HEADER_SAMPLE,
#     PATH_HEADER_PATIENT,
#     FNAME_CBIO_SID,
#     FNAME_SUMMARY_TEMPLATE_P,
#     FNAME_SUMMARY_TEMPLATE_S,
#     FNAME_SAMPLE_REMOVE
# )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating a template of patient and sample IDs for summary and timeline files")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    parser.add_argument(
        "--cbio_sample_list",
        action="store",
        dest="cbio_sample_list",
        required=True,
        help="location of sample list file",
    )
    parser.add_argument(
        "--sample_exclude_list",
        action="store",
        dest="sample_exclude_list",
        required=True,
        help="location of sample exclusion list file",
    )

    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)

    # Get template file paths from YAML (these are now table names for Databricks)
    fname_summary_header_template_patient = obj_yaml.return_template_info()['fname_cbio_header_template_p']
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']

    fname_summary_header_template_sample = obj_yaml.return_template_info()['fname_cbio_header_template_s']
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']

    # Get Databricks configuration
    databricks_config = obj_yaml.config_dict.get('inputs_databricks', {})
    catalog = databricks_config.get('catalog', 'cdsi_prod')
    schema = databricks_config.get('schema', 'cdm_impact_pipeline_prod')
    volume = databricks_config.get('volume', 'cdm_impact_pipeline')

    # Construct full table names for header templates (inputs)
    # Assuming header templates are stored as tables with same name structure
    table_header_patient = f"{catalog}.{schema}.cbioportal_summary_header_patient"
    table_header_sample = f"{catalog}.{schema}.cbioportal_summary_header_sample"

    # Construct full volume paths for output templates
    volume_path_template_patient = f"/Volumes/{catalog}/{schema}/{volume}/{fname_summary_template_patient}"
    volume_path_template_sample = f"/Volumes/{catalog}/{schema}/{volume}/{fname_summary_template_sample}"

    # Optional: table names for output (can be derived from file paths)
    table_template_patient = "data_clinical_patient_template_cdsi"
    table_template_sample = "data_clinical_sample_template_cdsi"

    FNAME_SAMPLE_REMOVE = args.sample_exclude_list
    FNAME_CBIO_SID = args.cbio_sample_list

    cbioportal_template_generator(
        env_databricks=args.databricks_env,
        table_header_sample=table_header_sample,
        table_header_patient=table_header_patient,
        fname_cbio_sid=FNAME_CBIO_SID,
        fname_sample_rmv=FNAME_SAMPLE_REMOVE,
        volume_path_summary_template_p=volume_path_template_patient,
        volume_path_summary_template_s=volume_path_template_sample,
        table_summary_template_p=table_template_patient,
        table_summary_template_s=table_template_sample,
        catalog=catalog,
        schema=schema
    )