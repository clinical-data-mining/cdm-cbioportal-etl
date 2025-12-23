"""
merge_intermediate_summaries.py

Script 2 of 4: Merge intermediate summary files into a single data file.

This script:
1. Loads the manifest CSV from Script 1
2. Loads the template from local filesystem
3. Merges all intermediate files horizontally (left join on ID column)
4. Saves merged data to both Databricks volume and table

Usage:
    python merge_intermediate_summaries.py \
        --manifest /Volumes/.../manifest_patient.csv \
        --databricks_env /path/to/databricks.env \
        --template /path/to/local/template.txt \
        --patient_or_sample patient \
        --output_volume_path /Volumes/.../data_clinical_patient_data.txt \
        --output_catalog cdsi_eng_phi \
        --output_schema cdm_eng_cbioportal_etl \
        --output_table data_clinical_patient
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from msk_cdm.databricks import DatabricksAPI


def load_template_from_local(fname_template: str, patient_or_sample: str) -> pd.DataFrame:
    """
    Load template from local filesystem and extract ID column.

    Parameters
    ----------
    fname_template : str
        Local filesystem path to template file
    patient_or_sample : str
        'patient' or 'sample'

    Returns
    -------
    pd.DataFrame
        Template with only ID column (deduplicated)
    """
    print(f"Loading template from local filesystem: {fname_template}")

    # Read from local file
    df_template = pd.read_csv(fname_template, sep='\t', dtype=str)

    # Determine ID columns to extract
    if patient_or_sample == 'patient':
        # Patient summaries: only PATIENT_ID
        id_columns = ['PATIENT_ID']
    else:
        # Sample summaries: both SAMPLE_ID and PATIENT_ID
        id_columns = ['SAMPLE_ID', 'PATIENT_ID']

    # Check all required columns exist
    missing_columns = [col for col in id_columns if col not in df_template.columns]
    if missing_columns:
        raise ValueError(
            f"Template missing required column(s): {missing_columns}. "
            f"Available columns: {list(df_template.columns)}"
        )

    # Extract ID columns
    df_template = df_template[id_columns].copy()

    # Drop duplicates
    original_rows = df_template.shape[0]
    df_template = df_template.drop_duplicates()
    deduplicated_rows = df_template.shape[0]

    if original_rows != deduplicated_rows:
        print(f"  Dropped {original_rows - deduplicated_rows} duplicate rows")

    print(f"  Loaded {deduplicated_rows} unique rows with columns: {id_columns}")

    return df_template


def merge_intermediates(
    df_manifest: pd.DataFrame,
    df_template: pd.DataFrame,
    obj_db: DatabricksAPI,
    patient_or_sample: str
) -> pd.DataFrame:
    """
    Merge all intermediate files horizontally.

    Parameters
    ----------
    df_manifest : pd.DataFrame
        Manifest with intermediate file paths
    df_template : pd.DataFrame
        Template with ID column
    obj_db : DatabricksAPI
        Databricks API object
    patient_or_sample : str
        'patient' or 'sample'

    Returns
    -------
    pd.DataFrame
        Merged data with all columns
    """
    print(f"\n{'='*80}")
    print(f"MERGING {len(df_manifest)} INTERMEDIATE FILES")
    print(f"{'='*80}\n")

    # Initialize with template
    # Merge key is always the primary ID for this level
    merge_key = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'
    df_merged = df_template.copy()

    template_columns = ', '.join(df_template.columns)
    print(f"Starting with template: {df_merged.shape[0]} rows, {df_merged.shape[1]} column(s) ({template_columns})")
    print(f"Merge key: {merge_key}")

    # Merge each intermediate
    for idx, row in df_manifest.iterrows():
        summary_id = row['summary_id']
        data_path = row['intermediate_data_path']

        print(f"\n[{idx + 1}/{len(df_manifest)}] Merging: {summary_id}")
        print(f"  Path: {data_path}")

        try:
            # Load intermediate data
            df_intermediate = obj_db.read_db_obj(volume_path=data_path, sep='\t')
            print(f"  Loaded: {df_intermediate.shape}")

            # Ensure merge key exists
            if merge_key not in df_intermediate.columns:
                print(f"  ⚠ WARNING: {merge_key} not found in intermediate. Skipping.")
                continue

            # Merge (left join from template)
            before_cols = df_merged.shape[1]
            df_merged = df_merged.merge(
                right=df_intermediate,
                how='left',
                on=merge_key
            )
            after_cols = df_merged.shape[1]
            new_cols = after_cols - before_cols

            print(f"  Added {new_cols} column(s)")
            print(f"  Current shape: {df_merged.shape}")

        except Exception as e:
            print(f"  ✗ ERROR merging {summary_id}: {str(e)}")
            continue

    print(f"\n{'='*80}")
    print(f"MERGE COMPLETE")
    print(f"{'='*80}")
    print(f"Final shape: {df_merged.shape[0]} rows × {df_merged.shape[1]} columns")
    print(f"Columns: {list(df_merged.columns)}")

    return df_merged


def save_merged_data(
    df_merged: pd.DataFrame,
    output_volume_path: str,
    output_catalog: str,
    output_schema: str,
    output_table: str,
    obj_db: DatabricksAPI
):
    """
    Save merged data to both Databricks volume and table.

    Parameters
    ----------
    df_merged : pd.DataFrame
        Merged data
    output_volume_path : str
        Databricks volume path
    output_catalog : str
        Catalog name
    output_schema : str
        Schema name
    output_table : str
        Table name
    obj_db : DatabricksAPI
        Databricks API object
    """
    print(f"\n{'='*80}")
    print(f"SAVING MERGED DATA")
    print(f"{'='*80}\n")

    # Prepare table info
    dict_database_table_info = {
        'catalog': output_catalog,
        'schema': output_schema,
        'table': output_table,
        'volume_path': output_volume_path,
        'sep': '\t'
    }

    print(f"Volume path: {output_volume_path}")
    print(f"Table:       {output_catalog}.{output_schema}.{output_table}")

    # Save to both volume and table
    obj_db.write_db_obj(
        df=df_merged,
        volume_path=output_volume_path,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    print(f"\n✓ Saved merged data:")
    print(f"  Volume: {output_volume_path}")
    print(f"  Table:  {output_catalog}.{output_schema}.{output_table}")
    print(f"  Shape:  {df_merged.shape}")


def main():
    parser = argparse.ArgumentParser(
        description="Merge intermediate summary files into a single data file"
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to manifest CSV from create_intermediate_summaries.py"
    )
    parser.add_argument(
        "--databricks_env",
        required=True,
        help="Path to Databricks environment file"
    )
    parser.add_argument(
        "--template",
        required=True,
        help="Local filesystem path to template file"
    )
    parser.add_argument(
        "--patient_or_sample",
        required=True,
        choices=['patient', 'sample'],
        help="Process patient or sample level summaries"
    )
    parser.add_argument(
        "--output_volume_path",
        required=True,
        help="Output Databricks volume path for merged data"
    )
    parser.add_argument(
        "--output_catalog",
        required=True,
        help="Catalog for output table"
    )
    parser.add_argument(
        "--output_schema",
        required=True,
        help="Schema for output table"
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="Table name for merged data"
    )

    args = parser.parse_args()

    print(f"\n{'#'*80}")
    print(f"# INTERMEDIATE SUMMARY MERGER")
    print(f"{'#'*80}")
    print(f"Manifest:            {args.manifest}")
    print(f"Template:            {args.template}")
    print(f"Patient/Sample:      {args.patient_or_sample}")
    print(f"Output volume:       {args.output_volume_path}")
    print(f"Output table:        {args.output_catalog}.{args.output_schema}.{args.output_table}")
    print(f"{'#'*80}\n")

    # Initialize Databricks API
    obj_db = DatabricksAPI(fname_databricks_env=args.databricks_env)

    # Load manifest
    print(f"Loading manifest: {args.manifest}")
    df_manifest = obj_db.read_db_obj(volume_path=args.manifest, sep=',')
    print(f"  Manifest has {len(df_manifest)} entries\n")

    # Load template from local filesystem
    df_template = load_template_from_local(
        fname_template=args.template,
        patient_or_sample=args.patient_or_sample
    )

    # Merge all intermediates
    df_merged = merge_intermediates(
        df_manifest=df_manifest,
        df_template=df_template,
        obj_db=obj_db,
        patient_or_sample=args.patient_or_sample
    )

    # Save merged data
    save_merged_data(
        df_merged=df_merged,
        output_volume_path=args.output_volume_path,
        output_catalog=args.output_catalog,
        output_schema=args.output_schema,
        output_table=args.output_table,
        obj_db=obj_db
    )

    print(f"\n{'#'*80}")
    print(f"# MERGE COMPLETE")
    print(f"{'#'*80}")
    print(f"Output: {args.output_volume_path}")
    print(f"Table:  {args.output_catalog}.{args.output_schema}.{args.output_table}")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()
