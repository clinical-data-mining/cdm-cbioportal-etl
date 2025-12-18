import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as config_cdm
from lib.utils import cbioportal_update_config, date_of_sequencing


fname_samples = config_cdm.fname_id_map

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating date of sequencing data")
    parser.add_argument(
        "--fname_save_date_of_seq",
        action="store",
        dest="fname_save_date_of_seq",
        help="File name to save date of sequencing data.",
    )
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    args = parser.parse_args()

    print('Generating date of sequencing data')
    date_of_sequencing(
        databricks_env=args.databricks_env,
        fname_samples=fname_samples,
        fname_save_date_of_seq=args.fname_save_date_of_seq
    )
    print('Complete')