import os
import sys
sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'utils')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', '..', 'cdm-utilities', 'minio_api/')))
import pandas as pd
from minio_api import MinioAPI
from data_classes_cdm import CDMProcessingVariables as config_cdm
from get_anchor_dates import get_anchor_dates
from constants import (
    ENV_MINIO,
    PATH_HEADER_SAMPLE,
    PATH_HEADER_PATIENT,
    FNAME_CBIO_SID,
    FNAME_SUMMARY_TEMPLATE_P,
    FNAME_SUMMARY_TEMPLATE_S
) 
FNAME_SAMPLE_REMOVE = os.path.abspath(os.path.join(os.path.dirname( __file__ ), "..", "tables", "mskimpact_clinical_data_remove.tsv"))


def remove_cases(df):
    # Remove cases without assay
    df_samples_rmv = pd.read_csv(
        FNAME_SAMPLE_REMOVE, 
        header=0, 
        sep='\t'
    )
    
    logic_keep = ~df['SAMPLE_ID'].isin(df_samples_rmv['SAMPLE_ID'])
    df_path_all_assays = df[logic_keep].copy()
    
    return df_path_all_assays
    

obj_minio = MinioAPI(
    fname_minio_env=ENV_MINIO
)

print('Loading header templates')
obj = obj_minio.load_obj(path_object=PATH_HEADER_SAMPLE)
df_header_template_s = pd.read_csv(obj, sep='\t')

obj = obj_minio.load_obj(path_object=PATH_HEADER_PATIENT)
df_header_template_p = pd.read_csv(obj, sep='\t')

# Load most current IDs -- 2024/01/22 moved to getting sample/patient IDs from msk-impact datahub. Expect at least a one day lag.
print('Loading current IMPACT IDs')
usecols=['SAMPLE_ID', 'PATIENT_ID']
df_id_current = pd.read_csv(
    config_cdm.fname_cbio_sid, 
    sep='\t', 
    header=4, 
    low_memory=False, 
    usecols=usecols
)

# Remove specific samples without assays 
df_id_current = remove_cases(df=df_id_current)
    

print('Creating cbioportal template files')
dict_patient = {'SAMPLE_ID':'#Sample Identifier', 'PATIENT_ID':'Patient Identifier'}
df_id_current_s = df_id_current.rename(columns=dict_patient)
df_f_s = pd.concat([df_header_template_s, df_id_current_s], axis=0)

dict_patient = {'PATIENT_ID':'#Patient Identifier'}
df_id_current_p = df_id_current[['PATIENT_ID']].drop_duplicates().rename(columns=dict_patient)
df_f_p = pd.concat([df_header_template_p, df_id_current_p], axis=0)

## Save data
print('Saving: %s' % FNAME_SUMMARY_TEMPLATE_P)
obj_minio.save_obj(
    df=df_f_p,
    path_object=FNAME_SUMMARY_TEMPLATE_P,
    sep='\t'
)

print('Saving: %s' % FNAME_SUMMARY_TEMPLATE_S)
obj_minio.save_obj(
    df=df_f_s,
    path_object=FNAME_SUMMARY_TEMPLATE_S,
    sep='\t'
)

