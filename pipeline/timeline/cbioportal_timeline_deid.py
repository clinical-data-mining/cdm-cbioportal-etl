# import os
# import sys
import argparse

# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# from variables import (
#     DICT_FILES_TIMELINE,
#     ENV_MINIO
# )
# from variables_testing_study import DICT_FILES_TIMELINE_TESTING
from cdm_cbioportal_etl.utils import yaml_config_parser
from cdm_cbioportal_etl.timeline import cbioportal_deid_timeline_files


def main():
    parser = argparse.ArgumentParser(description="Script for deidentifying timeline files for cbioportal.")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    obj_yaml = yaml_config_parser(fname_yaml_config=args.config_yaml)
    DICT_FILES_TIMELINE = obj_yaml.return_dict_phi_to_deid_timeline_production()
    DICT_FILES_TIMELINE_TESTING = obj_yaml.return_dict_phi_to_deid_timeline_testing()
    ENV_MINIO = obj_yaml.return_credential_filename()
    production_or_test = obj_yaml.return_production_or_test_indicator()



    if production_or_test == 'production':
        dict_files_timeline = DICT_FILES_TIMELINE
    elif args.production_or_test == 'test':
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING
    else:
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING


    _ = cbioportal_deid_timeline_files(
        fname_minio_env=ENV_MINIO,
        dict_files_timeline=dict_files_timeline
    )


if __name__ == '__main__':
    main()