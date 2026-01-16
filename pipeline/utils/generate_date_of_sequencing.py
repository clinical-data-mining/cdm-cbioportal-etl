import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.utils import date_of_sequencing

# Table names
TABLE_SAMPLES = 'cdsi_prod.cdm_impact_pipeline_prod.t03_id_mapping_pathology_sample_xml_parsed'

# Databricks volume configuration
CATALOG = 'cdsi_eng_phi'
SCHEMA = 'cdm_eng_cbioportal_etl'
VOLUME = 'cdm_eng_cbioportal_etl_volume'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for creating date of sequencing data")
    parser.add_argument(
        "--fname_save_date_of_seq",
        action="store",
        dest="fname_save_date_of_seq",
        help="File name to save date of sequencing data (deprecated - volume path is auto-generated).",
    )
    parser.add_argument(
        "--databricks_env",
        action="store",
        dest="databricks_env",
        required=True,
        help="--location of Databricks environment file",
    )
    args = parser.parse_args()

    # Construct volume path
    table_name = 'date_of_sequencing'
    volume_path_save = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/cbioportal/{table_name}.tsv'

    print('Generating date of sequencing data')
    date_of_sequencing(
        databricks_env=args.databricks_env,
        table_samples=TABLE_SAMPLES,
        volume_path_save_date_of_seq=volume_path_save,
        table_save_date_of_seq=table_name,
        catalog=CATALOG,
        schema=SCHEMA
    )
    print('Complete')