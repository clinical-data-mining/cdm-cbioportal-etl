import argparse

from msk_cdm.data_classes.epic_ddp_concat import CDMProcessingVariables as config_cdm
from cdm_cbioportal_etl.utils import cbioportal_update_config
from cdm_cbioportal_etl.utils import compute_age_at_sequencing

fname_save_age_at_seq = 'cbioportal/age_at_sequencing.tsv'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating data for OS")
    parser.add_argument(
        "--config_yaml",
        action="store",
        dest="config_yaml",
        help="Yaml file containing run parameters and necessary file locations.",
    )
    parser.add_argument(
        "--minio_env",
        action="store",
        dest="minio_env",
        required=True,
        help="--location of Minio environment file",
    )
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)

    fname_demo = config_cdm.fname_demo
    fname_samples = config_cdm.fname_id_map

    compute_age_at_sequencing(
        minio_env=args.minio_env,
        fname_demo=fname_demo,
        fname_samples=fname_samples,
        fname_save_age_at_seq=fname_save_age_at_seq
    )
