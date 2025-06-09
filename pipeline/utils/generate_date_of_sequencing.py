import argparse

from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as config_cdm
from cdm_cbioportal_etl.utils import cbioportal_update_config
from cdm_cbioportal_etl.utils import date_of_sequencing

fname_save_date_of_seq = 'cbioportal/seq_date.txt'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating data for OS")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--fname_save_date_of_seq",
        action="store",
        dest="fname_save_date_of_seq",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    args = parser.parse_args()

    fname_save_date_of_seq = args.fname_save_date_of_seq
    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()

    fname_samples = config_cdm.fname_id_map

    date_of_sequencing(
        minio_env=fname_minio_env,
        fname_samples=fname_samples,
        fname_save_date_of_seq=fname_save_date_of_seq
    )


