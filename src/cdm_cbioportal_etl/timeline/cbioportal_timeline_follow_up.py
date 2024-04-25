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
sys.path.insert(0,  os.path.abspath(os.path.join('', '..', 'utils')))
from minio_api import MinioAPI
from utils import (
    mrn_zero_pad, 
    print_df_without_index, 
    set_debug_console, 
    convert_to_int, 
    save_appended_df
)
from data_classes_cdm import CDMProcessingVariables as config_cdm
from data_classes_cdm import CDMProcessingVariablesCbioportal as config_cbio_etl


## Files to load
fname_minio_env = config_cdm.minio_env
fname_demo = config_cdm.fname_demo
fname_save = config_cbio_etl.fname_timeline_fu

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

## Create timeline file for follow-up
### Load data
print('Loading %s' % fname_demo)
obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
obj = obj_minio.load_obj(path_object=fname_demo)
df_demo = pd.read_csv(obj, sep='\t', usecols=col_keep)

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
