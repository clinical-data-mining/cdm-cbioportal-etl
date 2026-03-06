#!/usr/bin/env python3
"""
Create cBioPortal summary tables for Gleason scores at patient and sample levels.
"""
import argparse
import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from msk_cdm.databricks import DatabricksAPI

COL_GLEASON = 'GLEASON'
RENAME_SAMPLE = {COL_GLEASON: 'GLEASON_SAMPLE_LEVEL'}

# Hardcoded table paths from databricks_config_pathology.yaml
TABLE_GLEASON = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_timeline_gleason_scores'
TABLE_MAP = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'
table_name_patient = "table_summary_gleason_patient"
table_name_sample = "table_summary_gleason_sample"

# Output paths
CATALOG = 'cdsi_eng_phi'
SCHEMA = 'cdm_eng_pathology_report_segmentation'
VOLUME = 'cdm_eng_pathology_report_segmentation_volume'
SUBDIRECTORY = 'cbioportal'


def _load_data(obj_db, table_gleason, table_map):
    """Load Gleason and mapping data."""
    print(f'Loading {table_gleason}')
    sql_gleason = f"SELECT * FROM {table_gleason}"
    df_gleason = obj_db.query_from_sql(sql=sql_gleason)
    df_gleason['START_DATE'] = pd.to_datetime(df_gleason['START_DATE'], errors='coerce')

    print(f'Loading {table_map}')
    sql_map = f"SELECT MRN, SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {table_map}"
    df_map = obj_db.query_from_sql(sql=sql_map)

    return df_gleason, df_map


def _clean_data_patient(df_gleason):
    """Create patient-level summary with first and highest Gleason scores."""
    # Sort by MRN and date to get chronological order
    df_gleason_sorted = df_gleason.sort_values(by=['MRN', 'START_DATE'])

    # Get highest Gleason score for each patient
    gleason_highest = (df_gleason_sorted
                      .groupby('MRN')[COL_GLEASON]
                      .max()
                      .rename('GLEASON_HIGHEST_REPORTED')
                      .reset_index())

    # Get first Gleason score for each patient (chronologically)
    gleason_first = (df_gleason_sorted
                    .groupby('MRN')
                    .first()
                    .reset_index()
                    [['MRN', COL_GLEASON]]
                    .rename(columns={COL_GLEASON: 'GLEASON_FIRST_REPORTED'}))

    # Merge first and highest scores
    df_gleason_patient = gleason_first.merge(gleason_highest, on='MRN', how='inner')

    return df_gleason_patient


def _clean_data_sample(df_gleason):
    """Create sample-level summary mapping Gleason scores to sample IDs."""
    # Merge Gleason data with sample mapping using accession numbers
    df_gleason_samples = df_gleason[df_gleason['SAMPLE_ID'].notnull()].copy()
    df_gleason_samples = df_gleason_samples.drop_duplicates(subset=['SAMPLE_ID', COL_GLEASON])

    # Select and rename columns for output
    df_gleason_s = (df_gleason_samples[['SAMPLE_ID', COL_GLEASON]]
                   .rename(columns=RENAME_SAMPLE)
                   .copy())

    # Extract DMP_ID (first 9 characters of SAMPLE_ID)
    df_gleason_s['DMP_ID'] = df_gleason_s['SAMPLE_ID'].str[:9]

    return df_gleason_s


def create_gleason_summaries(
        obj_db,
        table_gleason,
        volume_path_patient,
        volume_path_sample,
        catalog,
        schema,
        table_name_patient,
        table_name_sample
):
    """Create and save Gleason patient and sample summaries."""
    # Load data
    df_gleason = _load_data(obj_db, table_gleason)

    # Create summaries
    df_gleason_p = _clean_data_patient(df_gleason=df_gleason)
    df_gleason_s = _clean_data_sample(df_gleason=df_gleason)

    # Save patient data
    print(f"Saving {len(df_gleason_p):,} patient records to {volume_path_patient}")
    print(f"Creating table {catalog}.{schema}.{table_name_patient}")
    dict_database_table_info_patient = {
        'catalog': catalog,
        'schema': schema,
        'table': table_name_patient,
        'volume_path': volume_path_patient,
        'sep': '\t'
    }
    obj_db.write_db_obj(
        df=df_gleason_p,
        volume_path=volume_path_patient,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info_patient
    )

    # Save sample data
    print(f"Saving {len(df_gleason_s):,} sample records to {volume_path_sample}")
    print(f"Creating table {catalog}.{schema}.{table_name_sample}")
    dict_database_table_info_sample = {
        'catalog': catalog,
        'schema': schema,
        'table': table_name_sample,
        'volume_path': volume_path_sample,
        'sep': '\t'
    }
    obj_db.write_db_obj(
        df=df_gleason_s,
        volume_path=volume_path_sample,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info_sample
    )

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create Gleason score summaries for cBioPortal"
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

    print('Creating Gleason Score Summaries')
    create_gleason_summaries(
        obj_db=obj_db,
        table_gleason=TABLE_GLEASON,
        volume_path_patient=volume_path_patient,
        volume_path_sample=volume_path_sample,
        catalog=CATALOG,
        schema=SCHEMA,
        table_name_patient=table_name_patient,
        table_name_sample=table_name_sample
    )


if __name__ == '__main__':
    main()
