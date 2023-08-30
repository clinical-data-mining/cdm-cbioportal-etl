import os
import sys

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "cdm-utilities", "minio_api")
    ),
)
from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int, save_appended_df
from get_anchor_dates import get_anchor_dates


FNAME_MINIO_ENV = config_rrpt.minio_env
# Dictionary of files to convert to a cbioportal timeline data file format, from a PHI version of it.
# Columns must AT LEAST contain the columns as defined in COLS_ORDER
DICT_FILES = {
    config_rrpt.fname_dx_timeline_prim: config_rrpt.fname_save_dx_prim_timeline,
    config_rrpt.fname_dx_timeline_met: config_rrpt.fname_save_dx_met_timeline,
    config_rrpt.fname_dx_timeline_ln: config_rrpt.fname_save_dx_ln_timeline,
    config_rrpt.fname_timeline_surg: config_rrpt.fname_save_surg_timeline,
    config_rrpt.fname_timeline_rt: config_rrpt.fname_save_rt_timeline,
    config_rrpt.fname_timeline_meds: config_rrpt.fname_save_meds_timeline
}
COLS_ORDER = ['PATIENT_ID', 'START_DATE', 'STOP_DATE', 'EVENT_TYPE', 'SUBTYPE']


def cbioportal_deid_timeline_files():
    df_path_g = get_anchor_dates()
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    for fname in DICT_FILES:
        print(fname)
        file_deid = DICT_FILES[fname]
        print(file_deid)
        obj = obj_minio.load_obj(path_object=fname)
        df_ = pd.read_csv(obj, header=0, low_memory=False, sep='\t')
        df_ = mrn_zero_pad(df=df_, col_mrn='MRN')

        if 'STOP_DATE' not in df_.columns:
            df_['STOP_DATE'] = ''

        df_['START_DATE'] = pd.to_datetime(df_['START_DATE'], errors='coerce') 
        df_['STOP_DATE'] = pd.to_datetime(df_['STOP_DATE'], errors='coerce') 

        # Merge deid date
        df_ = df_.merge(right=df_path_g, how='inner', on='MRN')
        df_ = df_.drop(columns=['MRN'])
        df_ = df_.rename(columns={'DMP_ID': 'PATIENT_ID'})

        # DeID dates
        start_date = (df_['START_DATE'] - df_['DTE_PATH_PROCEDURE']).dt.days
        stop_date = (df_['STOP_DATE'] - df_['DTE_PATH_PROCEDURE']).dt.days

        df_['START_DATE'] = start_date
        df_['STOP_DATE'] = stop_date
        df_ = df_.drop(columns=['DTE_PATH_PROCEDURE'])

        cols_other = [x for x in list(df_.columns) if x not in COLS_ORDER]
        cols_order_f = COLS_ORDER + cols_other

        df_ = df_[cols_order_f]

        save_appended_df(
            df=df_, 
            filename=file_deid, 
            sep='\t'
        )
        
        return None
    

def main():

    _ = cbioportal_deid_timeline_files()


if __name__ == '__main__':
    main()