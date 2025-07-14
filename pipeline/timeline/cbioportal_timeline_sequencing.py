""""
cbioportal_timeline_sequencing.py

Generates cBioPortal timeline files for sequencing dates
"""
import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as config_cdm_epic
from cdm_cbioportal_etl.utils import cbioportal_update_config


FNAME_SEQ_DATA = config_cdm_epic.fname_id_map
FNAME_SAVE_TIMELINE_SEQ = config_cdm.fname_path_sequencing_cbio_timeline
COL_DTE_SEQ = 'DTE_TUMOR_SEQUENCING'
COL_ORDER_SEQ = [
    'MRN', 
    'START_DATE', 
    'STOP_DATE', 
    'EVENT_TYPE', 
    'SUBTYPE',
    'SAMPLE_ID'
]


def sequencing_timeline(
        yaml_config
):
    obj_yaml = cbioportal_update_config(fname_yaml_config=yaml_config)
    fname_minio_env = obj_yaml.return_credential_filename()
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)
    
    print('Loading %s' % FNAME_SEQ_DATA)
    obj = obj_minio.load_obj(path_object=FNAME_SEQ_DATA)
    df_path = pd.read_csv(
        obj, 
        header=0, 
        low_memory=False, 
        sep='\t'
    )
    
    df_path = df_path.dropna().copy()
    df_path[COL_DTE_SEQ] = pd.to_datetime(
        df_path[COL_DTE_SEQ],
        errors='coerce'
    )
    log1 = df_path['SAMPLE_ID'].notnull()
    log2 = df_path['SAMPLE_ID'].str.contains('T')
    log3 = df_path['COL_DTE_SEQ'].notnull()  # Drop samples without sequencing date
    log = log1 & log2 & log3
    df_path_filt = df_path[log].copy()

    df_path_filt = df_path_filt.rename(columns={COL_DTE_SEQ: 'START_DATE'})
    df_path_filt = df_path_filt.assign(STOP_DATE='')
    df_path_filt = df_path_filt.assign(EVENT_TYPE='Sequencing')
    df_path_filt = df_path_filt.assign(SUBTYPE='')
    
    # Reorder columns
    df_samples_seq_f = df_path_filt[COL_ORDER_SEQ]
    
    df_samples_seq_f = df_samples_seq_f.dropna()
    
    # Save timeline
    print('Saving: %s' % FNAME_SAVE_TIMELINE_SEQ)
    obj_minio.save_obj(
        df=df_samples_seq_f,
        path_object=FNAME_SAVE_TIMELINE_SEQ,
        sep='\t'
    )

    return df_samples_seq_f

def main():
    parser = argparse.ArgumentParser(
        description="Script for creating timeline file for sequencing dates")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()
    yaml_config = args.config_yaml

    df_seq_timeline = sequencing_timeline(
        yaml_config=yaml_config
    )
    print(df_seq_timeline.sample())
    

if __name__ == '__main__':
    main()
    
    
