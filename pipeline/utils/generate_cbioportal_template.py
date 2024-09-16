import argparse
# import sys
# import os

from cdm_cbioportal_etl.summary import generate_cbioportal_template
from cdm_cbioportal_etl.utils import yaml_config_parser
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
    args = parser.parse_args()

    obj_yaml = yaml_config_parser(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()

    fname_summary_header_template_patient = obj_yaml.return_template_info()['fname_cbio_header_template_p']
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']

    fname_summary_header_template_sample = obj_yaml.return_template_info()['fname_cbio_header_template_s']
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']

    FNAME_SAMPLE_REMOVE = obj_yaml.return_sample_exclude_list()
    FNAME_CBIO_SID = obj_yaml.return_sample_list_filename()



    generate_cbioportal_template(
        env_minio=fname_minio_env,
        path_header_sample=fname_summary_header_template_sample,
        path_header_patient=fname_summary_header_template_patient,
        fname_cbio_sid=FNAME_CBIO_SID,
        fname_sample_rmv=FNAME_SAMPLE_REMOVE,
        fname_summary_template_p=fname_summary_template_patient,
        fname_summary_template_s=fname_summary_template_sample
    )