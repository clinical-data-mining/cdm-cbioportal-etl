import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.utils import compute_age_at_sequencing

# Table names
TABLE_DEMO = 'cdsi_prod.cdm_impact_pipeline_prod.t01_epic_ddp_demographics'
TABLE_SAMPLES = 'cdsi_prod.cdm_impact_pipeline_prod.t03_id_mapping_pathology_sample_xml_parsed'

# Databricks volume configuration
CATALOG = 'cdsi_eng_phi'
SCHEMA = 'cdm_eng_cbioportal_etl'
VOLUME = 'cdm_eng_cbioportal_etl_volume'

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

    # Construct volume path
    table_name = 'age_at_sequencing'
    volume_path_save = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/cbioportal/{table_name}.tsv'

    print('Generating age at sequencing data')
    compute_age_at_sequencing(
        databricks_env=args.databricks_env,
        table_demo=TABLE_DEMO,
        table_samples=TABLE_SAMPLES,
        volume_path_save_age_at_seq=volume_path_save,
        table_save_age_at_seq=table_name,
        catalog=CATALOG,
        schema=SCHEMA
    )
    print('Complete')
