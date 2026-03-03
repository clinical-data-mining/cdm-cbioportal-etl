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
TABLE_MAP = 'cdsi_eng_phi.cdm_eng_pathology_report_segmentation.table_pathology_impact_sample_summary_dop_anno_epic_idb_combined'

# Output paths
CATALOG = 'cdsi_eng_phi'
SCHEMA = 'cdm_eng_pathology_report_segmentation'
VOLUME = 'cdm_eng_pathology_report_segmentation_volume'
SUBDIRECTORY = 'cbioportal'


def _load_data(obj_db, table_pdl1, table_map):
    """Load PD-L1 and mapping data."""
    print(f'Loading {table_pdl1}')
    sql_pdl1 = f"SELECT * FROM {table_pdl1}"
    df_pdl1 = obj_db.query_from_sql(sql=sql_pdl1)
    df_pdl1['START_DATE'] = pd.to_datetime(df_pdl1['START_DATE'], errors='coerce')

    print(f'Loading {table_map}')
    sql_map = f"SELECT SAMPLE_ID, SOURCE_ACCESSION_NUMBER_0 FROM {table_map}"
    df_map = obj_db.query_from_sql(sql=sql_map)

    return df_pdl1, df_map


def _clean_data_patient(df_pdl1):
    """Create patient-level summary."""
    df_pdl1 = df_pdl1.sort_values(by=['MRN', 'START_DATE'])
    reps = {True: 'Yes', False: 'No'}
    list_mrns_pdl1 = df_pdl1.loc[df_pdl1['PDL1_POSITIVE'] == 'Yes', 'MRN']
    df_pdl1_summary = df_pdl1[['MRN']].drop_duplicates()
    df_pdl1_summary['HISTORY_OF_PDL1'] = df_pdl1_summary['MRN'].isin(list_mrns_pdl1).replace(reps)

    return df_pdl1_summary


def _clean_data_sample(df_pdl1, df_map):
    """Create sample-level summary."""
    df_pdl1_s1 = df_pdl1.merge(
        right=df_map[['SAMPLE_ID', 'SOURCE_ACCESSION_NUMBER_0']],
        how='inner',
        left_on='ACCESSION_NUMBER',
        right_on='SOURCE_ACCESSION_NUMBER_0'
    )

    df_pdl1_s = df_pdl1_s1[['SAMPLE_ID', 'PDL1_POSITIVE']].copy()
    df_pdl1_s['DMP_ID'] = df_pdl1_s['SAMPLE_ID'].apply(lambda x: x[:9])

    return df_pdl1_s


def create_pdl1_summaries(obj_db, table_pdl1, table_map,
                         volume_path_patient, volume_path_sample,
                         catalog, schema,
                         table_name_patient, table_name_sample):
    """Create and save PD-L1 patient and sample summaries."""
    # Load data
    df_pdl1, df_map = _load_data(obj_db, table_pdl1, table_map)

    # Create summaries
    df_pdl1_p = _clean_data_patient(df_pdl1=df_pdl1)
    df_pdl1_s = _clean_data_sample(df_pdl1=df_pdl1, df_map=df_map)

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
    volume_path_patient = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SUBDIRECTORY}/table_summary_pdl1_patient.tsv"
    volume_path_sample = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SUBDIRECTORY}/table_summary_pdl1_sample.tsv"

    table_name_patient = "table_summary_pdl1_patient"
    table_name_sample = "table_summary_pdl1_sample"

    print('Creating PD-L1 Summaries')
    create_pdl1_summaries(
        obj_db=obj_db,
        table_pdl1=TABLE_PDL1,
        table_map=TABLE_MAP,
        volume_path_patient=volume_path_patient,
        volume_path_sample=volume_path_sample,
        catalog=CATALOG,
        schema=SCHEMA,
        table_name_patient=table_name_patient,
        table_name_sample=table_name_sample
    )


if __name__ == '__main__':
    main()
