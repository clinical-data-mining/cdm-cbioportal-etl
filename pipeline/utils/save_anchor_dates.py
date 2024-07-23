"""
save_anchor_dates.py

This script will compute the anchor dates and save the file to MinIO
"""
import os
import sys
import argparse

import pandas as pd

from msk_cdm.minio import MinioAPI
from cdm_cbioportal_etl.utils.get_anchor_dates import get_anchor_dates

sys.path.insert(0,  os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..')))
from variables import ENV_MINIO
FNAME_SAVE_ANCHOR_DATES = 'cbioportal/timeline_anchor_dates.tsv'



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
        "--fname_minio_env",
        action="store",
        dest="fname_minio_env",
        default=ENV_MINIO,
        help="CSV file with Minio environment variables."
    )
    parser.add_argument(
        "--fname_save",
        action="store",
        dest="fname_save",
        default=FNAME_SAVE_ANCHOR_DATES,
        help="Minio location to save the anchor dates."
    )

    args = parser.parse_args()
    save_anchor_dates(
        fname_minio_env=args.fname_minio_env,
        fname_save=args.fname_save
    )
