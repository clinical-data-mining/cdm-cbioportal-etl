import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.summary import cbioportal_template_generator
from lib.utils import cbioportal_update_config


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

    # Get template filenames from YAML
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']

    # Get Databricks configuration
    databricks_config = obj_yaml.config_dict.get('inputs_databricks', {})
    catalog = databricks_config.get('catalog')
    schema = databricks_config.get('schema')
    volume = databricks_config.get('volume')
    volume_path_intermediate = databricks_config.get('volume_path_intermediate')

    # Local header template files (static config files)
    config_dir = os.path.join(os.path.dirname(__file__), '..', 'config', 'cbioportal_headers')
    fname_header_patient = os.path.join(config_dir, 'cbioportal_summary_header_patient.tsv')
    fname_header_sample = os.path.join(config_dir, 'cbioportal_summary_header_sample.tsv')

    # Construct full volume paths for output templates
    volume_path_template_patient = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}{fname_summary_template_patient}"
    volume_path_template_sample = f"/Volumes/{catalog}/{schema}/{volume}/{volume_path_intermediate}{fname_summary_template_sample}"

    # Optional: table names for output (can be derived from file paths)
    table_template_patient = "data_clinical_patient_template_cdsi"
    table_template_sample = "data_clinical_sample_template_cdsi"

    FNAME_SAMPLE_REMOVE = args.sample_exclude_list
    FNAME_CBIO_SID = args.cbio_sample_list

    cbioportal_template_generator(
        env_databricks=args.databricks_env,
        fname_header_sample=fname_header_sample,
        fname_header_patient=fname_header_patient,
        fname_cbio_sid=FNAME_CBIO_SID,
        fname_sample_rmv=FNAME_SAMPLE_REMOVE,
        volume_path_summary_template_p=volume_path_template_patient,
        volume_path_summary_template_s=volume_path_template_sample,
        table_summary_template_p=table_template_patient,
        table_summary_template_s=table_template_sample,
        catalog=catalog,
        schema=schema
    )