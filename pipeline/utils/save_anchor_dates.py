"""
save_anchor_dates.py

This script will compute the anchor dates and save the file to MinIO
"""
import os
import sys
import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as cdm_data
from cdm_cbioportal_etl.utils.get_anchor_dates import get_anchor_dates
from cdm_cbioportal_etl.utils import cbioportal_update_config

FNAME_SAVE_ANCHOR_DATES = cdm_data.fname_anchor_dates_reid



def save_anchor_dates(fname_minio_env, fname_save):

    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    # Anchor dates
    df_path_g = get_anchor_dates()

    print('Saving anchor dates: %s' % fname_save)
    # Save dataframe
    obj_minio.save_obj(
        df=df_path_g,
        path_object=fname_save,
        sep='\t'
    )

    print('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anchor dates used in the cBioPortal timeline files to deidentify")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()
    fname_save = FNAME_SAVE_ANCHOR_DATES

    args = parser.parse_args()
    save_anchor_dates(
        fname_minio_env=fname_minio_env,
        fname_save=fname_save
    )
