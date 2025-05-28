import argparse
# import sys
# import os

from cdm_cbioportal_etl.summary import cbioportal_template_generator
from cdm_cbioportal_etl.utils import cbioportal_update_config
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
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="--location of Minio environment file",
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

    fname_summary_header_template_patient = obj_yaml.return_template_info()['fname_cbio_header_template_p']
    fname_summary_template_patient = obj_yaml.return_template_info()['fname_p_sum_template_cdsi']

    fname_summary_header_template_sample = obj_yaml.return_template_info()['fname_cbio_header_template_s']
    fname_summary_template_sample = obj_yaml.return_template_info()['fname_s_sum_template_cdsi']

    FNAME_SAMPLE_REMOVE = args.sample_exclude_list
    FNAME_CBIO_SID = args.cbio_sample_list

    cbioportal_template_generator(
        env_minio=args.minio_env,
        path_header_sample=fname_summary_header_template_sample,
        path_header_patient=fname_summary_header_template_patient,
        fname_cbio_sid=FNAME_CBIO_SID,
        fname_sample_rmv=FNAME_SAMPLE_REMOVE,
        fname_summary_template_p=fname_summary_template_patient,
        fname_summary_template_s=fname_summary_template_sample
    )