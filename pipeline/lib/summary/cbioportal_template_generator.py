import pandas as pd

from msk_cdm.databricks import DatabricksAPI
from ..utils import constants

#Constants defined in python package for manifest file column names
COL_P_ID = constants.COL_P_ID_CBIO
COL_S_ID = constants.COL_S_ID_CBIO


def _remove_cases(df, fname_sample_rmv):
    # Remove cases without assay
    df_samples_rmv = pd.read_csv(
        fname_sample_rmv,
        header=0, 
        sep='\t'
    )
    
    logic_keep = ~df[COL_S_ID].isin(df_samples_rmv[COL_S_ID])
    df_path_all_assays = df[logic_keep].copy()
    
    return df_path_all_assays
    
def cbioportal_template_generator(
        env_databricks,
        fname_header_sample,
        fname_header_patient,
        fname_cbio_sid,
        fname_sample_rmv,
        volume_path_summary_template_p,
        volume_path_summary_template_s,
        table_summary_template_p=None,
        table_summary_template_s=None,
        catalog=None,
        schema=None
):
    obj_db = DatabricksAPI(
        fname_databricks_env=env_databricks
    )

    print('Loading header templates from local files')
    print('Sample header file: %s' % fname_header_sample)
    df_header_template_s = pd.read_csv(fname_header_sample, sep='\t', header=0)

    print('Patient header file: %s' % fname_header_patient)
    df_header_template_p = pd.read_csv(fname_header_patient, sep='\t', header=0)

    # Load most current IDs -- 2024/01/22 moved to getting sample/patient IDs from msk-impact datahub. Expect at least a one day lag.
    print('Loading current IMPACT IDs %s' % fname_cbio_sid)
    usecols=[COL_S_ID, COL_P_ID]
    df_id_current = pd.read_csv(
        fname_cbio_sid,
        sep='\t',
        header=0,
        low_memory=False,
        usecols=usecols
    )

    # Remove specific samples without assays
    df_id_current = _remove_cases(
        df=df_id_current,
        fname_sample_rmv=fname_sample_rmv
    )

    print('Creating cbioportal template files')
    dict_patient = {COL_S_ID:'#Sample Identifier', COL_P_ID:'Patient Identifier'}
    df_id_current_s = df_id_current.rename(columns=dict_patient)
    df_f_s = pd.concat([df_header_template_s, df_id_current_s], axis=0)

    dict_patient = {COL_P_ID:'#Patient Identifier'}
    df_id_current_p = df_id_current[[COL_P_ID]].drop_duplicates().rename(columns=dict_patient)
    df_f_p = pd.concat([df_header_template_p, df_id_current_p], axis=0)

    ## Save data to Databricks volumes
    print('Saving patient template to volume: %s' % volume_path_summary_template_p)

    # Prepare table info dictionary for patient template if table name provided
    obj_db.write_db_obj(
        df=df_f_p,
        volume_path=volume_path_summary_template_p,
        sep='\t',
        overwrite=True
    )

    print('Saving sample template to volume: %s' % volume_path_summary_template_s)

    # Prepare table info dictionary for sample template if table name provided
    obj_db.write_db_obj(
        df=df_f_s,
        volume_path=volume_path_summary_template_s,
        sep='\t',
        overwrite=True
    )

