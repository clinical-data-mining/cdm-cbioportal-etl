#!/usr/bin/env python3
"""
Create cBioPortal summary tables for PD-L1 at patient and sample levels.
"""
import argparse
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from msk_cdm.databricks import DatabricksAPI

# Hardcoded table paths from databricks_config_pathology.yaml
TABLE_PDL1 = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_timeline_pdl1_calls'

# Output paths
CATALOG = 'cdsi_eng_phi'
SCHEMA = 'cdm_eng_pathology_report_segmentation'
VOLUME = 'cdm_eng_pathology_report_segmentation_volume'
SUBDIRECTORY = 'cbioportal'
table_name_patient = "table_summary_pdl1_patient"
table_name_sample = "table_summary_pdl1_sample"


def _load_data(obj_db, table_pdl1):
    """Load PD-L1 and mapping data."""
    print(f'Loading {table_pdl1}')
    sql_pdl1 = f"SELECT * FROM {table_pdl1}"
    df_pdl1 = obj_db.query_from_sql(sql=sql_pdl1)
    df_pdl1['START_DATE'] = pd.to_datetime(df_pdl1['START_DATE'], errors='coerce')

    return df_pdl1


def _clean_data_patient(df_pdl1):
    """Create patient-level summary showing if patient ever had PDL1 positive result."""
      # Get all unique MRNs that have at least one PDL1 positive result
      mrns_with_pdl1_positive = df_pdl1.loc[df_pdl1['PDL1_POSITIVE'] == 'Yes', 'MRN'].unique()

      # Create summary with all unique MRNs
      df_pdl1_summary = df_pdl1[['MRN']].drop_duplicates().copy()

      # Mark HISTORY_OF_PDL1 as 'Yes' if MRN ever had a positive result
      df_pdl1_summary['HISTORY_OF_PDL1'] = df_pdl1_summary['MRN'].isin(mrns_with_pdl1_positive).map({True: 'Yes', False: 'No'})

      return df_pdl1_summary


def _clean_data_sample(df_pdl1):
    """Create sample-level summary showing PDL1 status for each sample."""
    # Filter to records with valid SAMPLE_ID and remove duplicates
    df_pdl1_samples = df_pdl1[df_pdl1['SAMPLE_ID'].notnull()].copy()
    df_pdl1_samples = df_pdl1_samples.drop_duplicates(subset=['SAMPLE_ID', 'PDL1_POSITIVE'])

    # Select relevant columns
    df_pdl1_s = df_pdl1_samples[['SAMPLE_ID', 'PDL1_POSITIVE']].copy()

    # Extract DMP_ID (first 9 characters of SAMPLE_ID)
    df_pdl1_s['DMP_ID'] = df_pdl1_s['SAMPLE_ID'].str[:9]

    return df_pdl1_s


def create_pdl1_summaries(
        obj_db,
        table_pdl1,
        volume_path_patient,
        volume_path_sample,
        catalog,
        schema,
        table_name_patient,
        table_name_sample
):
    """Create and save PD-L1 patient and sample summaries."""
    # Load data
    df_pdl1 = _load_data(obj_db, table_pdl1)

    # Create summaries
    df_pdl1_p = _clean_data_patient(df_pdl1=df_pdl1)
    df_pdl1_s = _clean_data_sample(df_pdl1=df_pdl1)

    # Save patient data
    print(f"Saving {len(df_pdl1_p):,} patient records to {volume_path_patient}")
    print(f"Creating table {catalog}.{schema}.{table_name_patient}")
    dict_database_table_info_patient = {
        'catalog': catalog,
        'schema': schema,
        'table': table_name_patient,
        'volume_path': volume_path_patient,
        'sep': '\t'
    }
    obj_db.write_db_obj(
        df=df_pdl1_p,
        volume_path=volume_path_patient,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info_patient
    )

    # Save sample data
    print(f"Saving {len(df_pdl1_s):,} sample records to {volume_path_sample}")
    print(f"Creating table {catalog}.{schema}.{table_name_sample}")
    dict_database_table_info_sample = {
        'catalog': catalog,
        'schema': schema,
        'table': table_name_sample,
        'volume_path': volume_path_sample,
        'sep': '\t'
    }
    obj_db.write_db_obj(
        df=df_pdl1_s,
        volume_path=volume_path_sample,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info_sample
    )

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create PD-L1 summaries for cBioPortal"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file",
    )
    args = parser.parse_args()

    # Initialize DatabricksAPI
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Construct output paths
    volume_path_patient = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SUBDIRECTORY}/{table_name_patient}.tsv"
    volume_path_sample = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SUBDIRECTORY}/{table_name_sample}.tsv"

    print('Creating PD-L1 Summaries')
    create_pdl1_summaries(
        obj_db=obj_db,
        table_pdl1=TABLE_PDL1,
        volume_path_patient=volume_path_patient,
        volume_path_sample=volume_path_sample,
        catalog=CATALOG,
        schema=SCHEMA,
        table_name_patient=table_name_patient,
        table_name_sample=table_name_sample
    )


if __name__ == '__main__':
    main()
