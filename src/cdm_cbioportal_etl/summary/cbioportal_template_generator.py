import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils import constants

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
    
def generate_cbioportal_template(
        env_minio,
        path_header_sample,
        path_header_patient,
        fname_cbio_sid,
        fname_sample_rmv,
        fname_summary_template_p,
        fname_summary_template_s
):
    obj_minio = MinioAPI(
        fname_minio_env=env_minio
    )

    print('Loading header templates')
    obj = obj_minio.load_obj(path_object=path_header_sample)
    df_header_template_s = pd.read_csv(obj, sep='\t')

    obj = obj_minio.load_obj(path_object=path_header_patient)
    df_header_template_p = pd.read_csv(obj, sep='\t')

    # Load most current IDs -- 2024/01/22 moved to getting sample/patient IDs from msk-impact datahub. Expect at least a one day lag.
    print('Loading current IMPACT IDs')
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

    ## Save data
    print('Saving: %s' % fname_summary_template_p)
    obj_minio.save_obj(
        df=df_f_p,
        path_object=fname_summary_template_p,
        sep='\t'
    )

    print('Saving: %s' % fname_summary_template_s)
    obj_minio.save_obj(
        df=df_f_s,
        path_object=fname_summary_template_s,
        sep='\t'
    )

