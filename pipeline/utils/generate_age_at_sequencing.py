import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as config_cdm
from lib.utils import cbioportal_update_config, compute_age_at_sequencing

fname_save_age_at_seq = 'cbioportal/age_at_sequencing.tsv'
fname_demo = config_cdm.fname_demo
fname_samples = config_cdm.fname_id_map


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating data for OS")
    parser.add_argument(
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="--location of Minio environment file",
    )
    args = parser.parse_args()

    print('Generating age at sequencing data')
    compute_age_at_sequencing(
        minio_env=args.minio_env,
        fname_demo=fname_demo,
        fname_samples=fname_samples,
        fname_save_age_at_seq=fname_save_age_at_seq
    )
    print('Complete')
