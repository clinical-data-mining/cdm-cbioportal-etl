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
    parser = argparse.ArgumentParser(description="Script for creating age at sequencing data")
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    args = parser.parse_args()

    print('Generating age at sequencing data')
    compute_age_at_sequencing(
        databricks_env=args.databricks_env,
        fname_demo=fname_demo,
        fname_samples=fname_samples,
        fname_save_age_at_seq=fname_save_age_at_seq
    )
    print('Complete')
