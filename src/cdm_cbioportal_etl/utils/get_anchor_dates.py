import pandas as pd

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_rrpt
from msk_cdm.minio import MinioAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad


FNAME_MINIO_ENV = config_rrpt.minio_env
FNAME_PATHOLOGY = config_rrpt.fname_path_clean
COLS_PATHOLOGY = ['MRN', 'DTE_TUMOR_SEQUENCING', 'SAMPLE_ID', 'DMP_ID']


def get_anchor_dates():
    print('Creating anchor date table from first sequencing date..')
    obj_minio = MinioAPI(fname_minio_env=FNAME_MINIO_ENV)
    
    print('Loading %s' % FNAME_PATHOLOGY)
    obj = obj_minio.load_obj(path_object=FNAME_PATHOLOGY)
    df_path = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=COLS_PATHOLOGY)
    
    df_path = df_path.dropna().copy()
    df_path['DTE_TUMOR_SEQUENCING'] = pd.to_datetime(
        df_path['DTE_TUMOR_SEQUENCING'],
        errors='coerce'
    )
    
    df_path_filt = df_path[df_path['SAMPLE_ID'].notnull() & df_path['SAMPLE_ID'].str.contains('T')]
    df_path_filt['DMP_ID_DERIVED'] = df_path_filt['SAMPLE_ID'].apply(lambda x: x[:9])

    # Remove cases where DMP_ID does not match DMP_ID Derived
    df_path_sample_id_error = df_path_filt[df_path_filt['DMP_ID_DERIVED'] != df_path_filt['DMP_ID']]
    df_path_filt = df_path_filt[df_path_filt['DMP_ID_DERIVED'] == df_path_filt['DMP_ID']]


    df_path_filt = df_path_filt.sort_values(by=['MRN', 'DTE_TUMOR_SEQUENCING'])
    df_path_g = df_path_filt.groupby(['MRN', 'DMP_ID'])['DTE_TUMOR_SEQUENCING'].first().reset_index()
    
    df_path_g = mrn_zero_pad(df=df_path_g, col_mrn='MRN')
    
    # Remove mismapped cases
    filt_mismapped = df_path_g['MRN'].duplicated(keep=False) | df_path_g['DMP_ID'].duplicated(keep=False)
    df_path_g_f = df_path_g[~filt_mismapped]
    
    df_path_g_error = df_path_g[filt_mismapped].copy()
    print('Error in mapping summary:')
    print(df_path_g_error)
    print(df_path_sample_id_error)
    
    print('Done!')
    
    return df_path_g_f
