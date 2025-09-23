import argparse

import pandas as pd

from cdm_cbioportal_etl.utils import cbioportal_update_config
from cdm_cbioportal_etl.timeline import cbioportal_deid_timeline_files
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as cdm_files
from msk_cdm.data_processing import mrn_zero_pad

COL_OS_DATE = 'OS_DATE'
COL_ID = 'MRN'

def compute_os_date(fname_minio_env, fname_demo):
    print('Loading %s' % fname_demo)
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t')

    df_demo = mrn_zero_pad(df=df_demo, col_mrn=COL_ID)
    df_demo['PLA_LAST_CONTACT_DTE'] = pd.to_datetime(df_demo['PLA_LAST_CONTACT_DTE'], errors='coerce')
    df_demo['PT_DEATH_DTE'] = pd.to_datetime(df_demo['PT_DEATH_DTE'], errors='coerce')

    df_demo[COL_OS_DATE] = df_demo['PT_DEATH_DTE'].fillna(df_demo['PLA_LAST_CONTACT_DTE'])

    log_err = df_demo['PT_DEATH_DTE'] < df_demo['PLA_LAST_CONTACT_DTE']
    df_demo.loc[log_err, COL_OS_DATE] = df_demo['PT_DEATH_DTE']

    return df_demo

def main():
    parser = argparse.ArgumentParser(description="Script for deidentifying timeline files for cbioportal.")
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
        help="location of Minio environment file",
    )
    parser.add_argument(
        "--cbio_sample_list",
        action="store",
        dest="cbio_sample_list",
        required=True,
        help="location of sample list file",
    )
    parser.add_argument(
        "--path_datahub",
        action="store",
        dest="path_datahub",
        required=True,
        help="path to datahub",
    )
    parser.add_argument(
        "--production_or_test",
        action="store",
        dest="production_or_test",
        required=True,
        choices=["test", "production"],
        help="Enter test or production to indicate the columns/files to use for portal file generation",
    )

    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    DICT_FILES_TIMELINE = obj_yaml.return_dict_phi_to_deid_timeline_production(path_datahub=args.path_datahub)
    DICT_FILES_TIMELINE_TESTING = obj_yaml.return_dict_phi_to_deid_timeline_testing(path_datahub=args.path_datahub)
    fname_metadata = obj_yaml.return_filename_codebook_metadata()
    fname_tables = obj_yaml.return_filename_codebook_tables()
    ENV_MINIO = args.minio_env
    production_or_test = args.production_or_test
    fname_demo = cdm_files.fname_demo

    # Get list of sample and patient IDs used for cbioportal ETL
    fname_sample = args.cbio_sample_list
    print(f'Loading {fname_sample}')
    df_samples_used = pd.read_csv(fname_sample, sep='\t')
    list_dmp_ids = list(df_samples_used['PATIENT_ID'].drop_duplicates())
    list_sample_ids = list(df_samples_used['SAMPLE_ID'].drop_duplicates())
    print('Number of sample ids in timeline template: %s' % str(len(list_sample_ids)))
    print('Number of patients in timeline template: %s' % str(len(list_dmp_ids)))

    if production_or_test == 'production':
        dict_files_timeline = DICT_FILES_TIMELINE
    elif production_or_test == 'test':
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING
    else:
        dict_files_timeline = DICT_FILES_TIMELINE_TESTING

    df_patient_os_date = compute_os_date(
        fname_minio_env=ENV_MINIO,
        fname_demo=fname_demo
    )


    _ = cbioportal_deid_timeline_files(
        fname_minio_env=ENV_MINIO,
        dict_files_timeline=dict_files_timeline,
        list_dmp_ids=list_dmp_ids,
        list_sample_ids=list_sample_ids,
        df_patient_os_date=df_patient_os_date,
        col_os_date=COL_OS_DATE,
        col_id=COL_ID,
        fname_metadata=fname_metadata,
        fname_tables=fname_tables
    )



if __name__ == '__main__':
    main()