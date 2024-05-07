import os
import sys

import pandas as pd

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from variables import (
    ENV_MINIO,
    FNAME_DEMO,
    FNAME_TIMELINE_FU
)
from msk_cdm.minio import MinioAPI


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
print('Loading %s' % FNAME_DEMO)
obj_minio = MinioAPI(fname_minio_env=ENV_MINIO)
obj = obj_minio.load_obj(path_object=FNAME_DEMO)
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
print('Saving %s to MinIO' % FNAME_TIMELINE_FU)
obj_minio.save_obj(
    df=df_os_,
    path_object=FNAME_TIMELINE_FU,
    sep='\t'
)
