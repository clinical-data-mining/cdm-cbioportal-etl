import os
import sys

import pandas as pd

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "cdm-utilities")
    ),
)
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "cdm-utilities", "minio_api")
    ),
)
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
from data_classes_cdm import CDMProcessingVariables as config_rrpt
from minio_api import MinioAPI
from utils import mrn_zero_pad, print_df_without_index, set_debug_console, convert_to_int, save_appended_df
from get_anchor_dates import get_anchor_dates
from constants import (
    COLS_ORDER_CEA,
    FILE_CEA,
    FNAME_SAVE_CEA,
    ENV_MINIO
) 


def cbioportal_timeline_cea_labs():
    df_path_g = get_anchor_dates()
    obj_minio = MinioAPI(fname_minio_env=ENV_MINIO)
    
    # Load and clean lab data to only contains CEA values
    print('Loading %s' % FILE_CEA)
    obj = obj_minio.load_obj(path_object=FILE_CEA)
    df_ = pd.read_csv(obj, header=0, low_memory=False, sep=',')
    
    df_cea = df_[df_['LR_TEST_NAME'].str.contains('CEA')].copy()
    df_cea['LR_PERFORMED_DTE'] = pd.to_datetime(df_cea['LR_PERFORMED_DTE'], errors='coerce') 
    df_cea = mrn_zero_pad(df=df_cea, col_mrn='LR_MRN')
    cols_cea = ['LR_MRN', 'LR_PERFORMED_DTE', 'LR_RESULT_VALUE']
    df_cea['LR_RESULT_VALUE'] = pd.to_numeric(df_cea['LR_RESULT_VALUE'], errors='coerce').fillna(0.49)
    df_cea = df_cea[cols_cea]
    
    # Merge
    df_cea_f = df_cea.merge(right=df_path_g, how='inner', left_on='LR_MRN', right_on='MRN')
    start_date = (df_cea_f['LR_PERFORMED_DTE'] - df_cea_f['DTE_PATH_PROCEDURE']).dt.days
    df_cea_f = df_cea_f.rename(
        columns={
            'LR_RESULT_VALUE': 'RESULT',
            'DMP_ID': 'PATIENT_ID'
        }
    )

    df_cea_f = df_cea_f.assign(START_DATE=start_date)
    df_cea_f = df_cea_f.assign(STOP_DATE='')
    df_cea_f = df_cea_f.assign(EVENT_TYPE='LAB_TEST')
    df_cea_f = df_cea_f.assign(TEST='CEA')

    df_cea_f = df_cea_f[COLS_ORDER_CEA]
    
    # Save data to datahub repo
    save_appended_df(
        df=df_cea_f, 
        filename=FNAME_SAVE_CEA, 
        sep='\t'
    )
    
    return None


def main():

    _ = cbioportal_timeline_cea_labs()


if __name__ == '__main__':
    main()