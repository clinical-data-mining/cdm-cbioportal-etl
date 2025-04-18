import pandas as pd

# from msk_cdm.data_classes.legacy import CDMProcessingVariables as c_var
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as c_var
from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

FNAME_MINIO_ENV = c_var.minio_env
FNAME_PATHOLOGY = c_var.fname_id_map
COLS_PATHOLOGY = [
    'MRN',
    'DTE_TUMOR_SEQUENCING',
    'SAMPLE_ID',
    'DMP_ID'
]


def get_anchor_dates():
    print('Creating anchor date table from first sequencing date..')
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    print('Loading %s' % FNAME_PATHOLOGY)
    obj = obj_minio.load_obj(path_object=FNAME_PATHOLOGY)
    df_path = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=COLS_PATHOLOGY)
    df_path = mrn_zero_pad(df=df_path, col_mrn='MRN')
    
    df_path = df_path.dropna().copy()
    df_path['DTE_TUMOR_SEQUENCING'] = pd.to_datetime(
        df_path['DTE_TUMOR_SEQUENCING'],
        errors='coerce'
    )
    print(df_path.head())
    logic_filt1 = df_path['SAMPLE_ID'].notnull()
    logic_filt2 = df_path['SAMPLE_ID'].str.contains('T')
    logic_filt3 = df_path['DTE_TUMOR_SEQUENCING'].notnull()
    logic_filt = logic_filt1 & logic_filt2 & logic_filt3

    df_path_filt = df_path[logic_filt].copy()
    print(df_path_filt.head())

    df_path_filt['DMP_ID_DERIVED'] = df_path_filt['SAMPLE_ID'].apply(lambda x: x[:9])

    print(df_path_filt.head())

    # Remove cases where DMP_ID does not match DMP_ID Derived
    df_path_sample_id_error = df_path_filt[df_path_filt['DMP_ID_DERIVED'] != df_path_filt['DMP_ID']]
    print(df_path_sample_id_error.head())

    df_path_filt_clean1 = df_path_filt[df_path_filt['DMP_ID_DERIVED'] == df_path_filt['DMP_ID']]
    df_path_filt_clean1 = df_path_filt_clean1.sort_values(by=['MRN', 'DTE_TUMOR_SEQUENCING'])

    print(f"df_path_filt_clean1: {df_path_filt_clean1.head()}")

    df_path_g = df_path_filt_clean1.groupby(['MRN', 'DMP_ID'])['DTE_TUMOR_SEQUENCING'].first().reset_index()

    print(f"df_path_g: {df_path_g.head()}")

    # Remove any MRN or DMP-ID in df_path_g_error
    filt_rmv_patients = df_path_g['DMP_ID'].isin(df_path_sample_id_error['DMP_ID']) | \
                        df_path_g['MRN'].isin(list(set(df_path_sample_id_error['MRN']))) | \
                        df_path_g['DMP_ID'].isin(list(set(df_path_sample_id_error['DMP_ID_DERIVED'])))

    df_path_g_f = df_path_g[~filt_rmv_patients]

    print(f"df_path_g_f: {df_path_g_f.head()}")

    print('Error in mapping summary:')
    print(df_path_sample_id_error.shape)
    
    print('Done!')
    
    return df_path_g_f
