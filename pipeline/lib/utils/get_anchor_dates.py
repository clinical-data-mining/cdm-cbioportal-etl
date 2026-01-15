import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

# Default table name for pathology data (can be overridden)
TABLE_PATHOLOGY = 'cdsi_prod.cdm_impact_pipeline_prod.t03_id_mapping_pathology_sample_xml_parsed'
COL_SEQ_DATE = 'DATE_TUMOR_SEQUENCING'
COLS_PATHOLOGY = [
    'MRN',
    COL_SEQ_DATE,
    'SAMPLE_ID',
    'DMP_ID'
]


def get_anchor_dates(fname_databricks_env, table_pathology=TABLE_PATHOLOGY):
    print('Creating anchor date table from first sequencing date..')
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    print('Loading %s' % table_pathology)
    cols_str = ', '.join(COLS_PATHOLOGY)
    sql = f"SELECT {cols_str} FROM {table_pathology}"
    df_path = obj_db.query_from_sql(sql=sql)
    df_path = mrn_zero_pad(df=df_path, col_mrn='MRN')
    
    df_path = df_path.dropna().copy()
    df_path[COL_SEQ_DATE] = pd.to_datetime(
        df_path[COL_SEQ_DATE],
        errors='coerce'
    )

    logic_filt1 = df_path['SAMPLE_ID'].notnull()
    logic_filt2 = df_path['SAMPLE_ID'].str.contains('T')
    logic_filt3 = df_path[COL_SEQ_DATE].notnull()
    logic_filt = logic_filt1 & logic_filt2 & logic_filt3

    df_path_filt = df_path[logic_filt].copy()
    df_path_filt['DMP_ID_DERIVED'] = df_path_filt['SAMPLE_ID'].apply(lambda x: x[:9])

    # Find DMP_IDs that do not match DMP_ID Derived
    mrn_test1 = df_path_filt.groupby(['MRN'])['DMP_ID'].nunique()
    list_id_prob1 = list(mrn_test1[mrn_test1 > 1].index)

    mrn_test2 = df_path_filt.groupby(['DMP_ID'])['MRN'].nunique()
    list_id_prob2 = list(mrn_test2[mrn_test2 > 1].index)

    df_prob1 = df_path_filt[df_path_filt['DMP_ID_DERIVED'] != df_path_filt['DMP_ID']]
    df_prob2 = df_path_filt[df_path_filt['DMP_ID'].isin(list_id_prob2) | df_path_filt['MRN'].isin(list_id_prob1)]
    df_path_sample_id_error = pd.concat([df_prob1, df_prob2], axis=0).drop_duplicates()

    df_path_filt_clean1 = df_path_filt[df_path_filt['DMP_ID_DERIVED'] == df_path_filt['DMP_ID']]
    df_path_filt_clean1 = df_path_filt_clean1.sort_values(by=['MRN', COL_SEQ_DATE])

    df_path_g = df_path_filt_clean1.groupby(['MRN', 'DMP_ID'])[COL_SEQ_DATE].min().reset_index()
    # Keep only the date portion to drop time and timezone information
    df_path_g[COL_SEQ_DATE] = df_path_g[COL_SEQ_DATE].dt.date

    # Remove any MRN or DMP-ID in df_path_g_error
    filt_rmv_patients = df_path_g['DMP_ID'].isin(df_path_sample_id_error['DMP_ID']) | \
                        df_path_g['MRN'].isin(list(set(df_path_sample_id_error['MRN']))) | \
                        df_path_g['DMP_ID'].isin(list(set(df_path_sample_id_error['DMP_ID_DERIVED'])))

    df_path_g_f = df_path_g[~filt_rmv_patients]

    print('Error in mapping summary:')
    print(df_path_sample_id_error)
    
    print('Done!')
    
    return df_path_g_f
