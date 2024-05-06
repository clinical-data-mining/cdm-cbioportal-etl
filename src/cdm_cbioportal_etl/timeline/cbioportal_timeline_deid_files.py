import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import get_anchor_dates
from msk_cdm.data_processing import (
    mrn_zero_pad, 
    print_df_without_index, 
    set_debug_console, 
    convert_to_int, 
    save_appended_df
)


def cbioportal_deid_timeline_files(
    fname_minio_env,
    dict_files_timeline
):
    df_path_g = get_anchor_dates()
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    
    for fname in dict_files_timeline:
        print(fname)
        file_deid = dict_files_timeline[fname]
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
        df_ = df_[df_['START_DATE'].notnull()]
        df_ = convert_to_int(
            df=df_,
            list_cols=['START_DATE', 'STOP_DATE']
        )

        cols_other = [x for x in list(df_.columns) if x not in COLS_ORDER_GENERAL]
        cols_order_f = COLS_ORDER_GENERAL + cols_other

        df_ = df_[cols_order_f]

        save_appended_df(
            df=df_, 
            filename=file_deid, 
            sep='\t'
        )
        
    return None
    
