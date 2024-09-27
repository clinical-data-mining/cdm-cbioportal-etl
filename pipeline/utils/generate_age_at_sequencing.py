import argparse

from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
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
    args = parser.parse_args()

    obj_yaml = cbioportal_update_config(fname_yaml_config=args.config_yaml)
    fname_minio_env = obj_yaml.return_credential_filename()

    fname_demo = config_cdm.fname_demo
    fname_samples = config_cdm.fname_path_clean

    compute_age_at_sequencing(
        minio_env=fname_minio_env,
        fname_demo=fname_demo,
        fname_samples=fname_samples,
        fname_save_age_at_seq=fname_save_age_at_seq
    )

