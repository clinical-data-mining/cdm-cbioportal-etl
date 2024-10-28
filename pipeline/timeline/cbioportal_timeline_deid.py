import argparse

import pandas as pd

from cdm_cbioportal_etl.utils import cbioportal_update_config
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

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    DICT_FILES_TIMELINE = obj_yaml.return_dict_phi_to_deid_timeline_production()
    DICT_FILES_TIMELINE_TESTING = obj_yaml.return_dict_phi_to_deid_timeline_testing()
    ENV_MINIO = obj_yaml.return_credential_filename()
    production_or_test = obj_yaml.return_production_or_test_indicator()

    # Get list of sample and patient IDs used for cbioportal ETL
    fname_sample = obj_yaml.return_sample_list_filename()
    df_samples_used = pd.read_csv(fname_sample, sep='\t')
    list_dmp_ids = list(df_samples_used['PATIENT_ID'].drop_duplicates())

    if production_or_test == 'production':
        dict_files_timeline = DICT_FILES_TIMELINE
    elif production_or_test == 'test':
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING
    else:
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING


    _ = cbioportal_deid_timeline_files(
        fname_minio_env=ENV_MINIO,
        dict_files_timeline=dict_files_timeline,
        list_dmp_ids=list_dmp_ids
    )



if __name__ == '__main__':
    main()