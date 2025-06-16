import argparse

import numpy as np
# import os
# import sys

import pandas as pd

# sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
# from variables import (
#     ENV_MINIO,
#     FNAME_DEMO,
#     FNAME_TIMELINE_FU
# )
from cdm_cbioportal_etl.utils import cbioportal_update_config
from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as cdm_files


## Constants
col_keep = ['MRN', 'MRN_CREATE_DTE', 'PLA_LAST_CONTACT_DTE', 'PT_DEATH_DTE']
col_order = ['MRN', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SUBTYPE', 'SOURCE']
rep_dict = {
    'SOURCE': {
        'MRN_CREATE_DTE': 'First Consult',
        'PT_DEATH_DTE': 'Patient Deceased',
        'PLA_LAST_CONTACT_DTE': 'Last Contact'
    }
}

def cbioportal_timeline_follow_up(
        yaml_config,
        fname_demo,
        fname_save,
        fname_minio_env
):
    print('Parsing config file %s' % yaml_config)
    obj_yaml = cbioportal_update_config(fname_yaml_config=yaml_config)

    ## Create timeline file for follow-up
    ### Load data
    print('Loading %s' % fname_demo)
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    obj = obj_minio.load_obj(path_object=fname_demo)
    df_demo = pd.read_csv(obj, sep='\t', usecols=col_keep)

    df_demo_f = df_demo.copy()
    # Remove last contact date if patient is deceased
    logic_deceased = df_demo_f['PT_DEATH_DTE'].notnull()
    df_demo_f.loc[logic_deceased, 'PLA_LAST_CONTACT_DTE'] = pd.NA

    print('Creating timeline')
    df_os_ = pd.melt(
        frame=df_demo,
        id_vars='MRN',
        value_vars=[
            'MRN_CREATE_DTE',
            'PLA_LAST_CONTACT_DTE',
            'PT_DEATH_DTE'
        ],
        value_name='START_DATE',
        var_name='SOURCE'
    )

    df_os_ = df_os_.assign(STOP_DATE='')
    df_os_ = df_os_.assign(EVENT_TYPE='Diagnosis')
    df_os_ = df_os_.assign(SUBTYPE='Follow-Up')

    df_os_ = df_os_[df_os_['START_DATE'].notnull()].copy()
    df_os_ = df_os_[col_order]
    df_os_ = df_os_.replace(rep_dict)

    ### Save data
    print('Saving %s to MinIO' % fname_save)
    obj_minio.save_obj(
        df=df_os_,
        path_object=fname_save,
        sep='\t'
    )

    return None

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
    args = parser.parse_args()

    fname_demo = cdm_files.fname_demo
    fname_fu_save = cdm_files.fname_timeline_fu

    cbioportal_timeline_follow_up(
        yaml_config=args.config_yaml,
        fname_demo=fname_demo,
        fname_save=fname_fu_save,
        fname_minio_env=args.minio_env
    )
