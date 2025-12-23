"""
create_summary_header.py

Script 3 of 4: Create cBioPortal header from YAML configurations.

This script:
1. Loads the manifest CSV from Script 1
2. Loads merged data from Script 2 to determine column order
3. Extracts metadata from YAML configs
4. Creates header in tall format (4 columns × N rows)
5. Saves header to both Databricks volume and table

The tall format is:
    column_name | display_label | datatype | description
    PATIENT_ID  | #Patient Identifier | STRING | 1
    AGE         | Age at Sequencing | NUMBER | Age in years...

This format is database-friendly and will be transposed in Script 4.

Usage:
    python create_summary_header.py \
        --manifest /Volumes/.../manifest_patient.csv \
        --databricks_env /path/to/databricks.env \
        --merged_data_path /Volumes/.../data_clinical_patient_data.txt \
        --patient_or_sample patient \
        --output_volume_path /Volumes/.../data_clinical_patient_header.txt \
        --output_catalog cdsi_eng_phi \
        --output_schema cdm_eng_cbioportal_etl \
        --output_table data_clinical_patient_header
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import yaml
import pandas as pd
from typing import List, Dict
from msk_cdm.databricks import DatabricksAPI


def create_header_from_yamls(
    df_manifest: pd.DataFrame,
    df_merged_data: pd.DataFrame,
    patient_or_sample: str
) -> pd.DataFrame:
    """
    Create header in tall format from YAML configs.

    Parameters
    ----------
    df_manifest : pd.DataFrame
        Manifest with YAML config paths
    df_merged_data : pd.DataFrame
        Merged data to determine column order
    patient_or_sample : str
        'patient' or 'sample'

    Returns
    -------
    pd.DataFrame
        Header in tall format with columns:
        - column_name
        - display_label
        - datatype
        - description
    """
    print(f"\n{'='*80}")
    print(f"CREATING HEADER FROM {len(df_manifest)} YAML CONFIGS")
    print(f"{'='*80}\n")

    # Determine ID column
    id_column = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'
    id_label = '#Patient Identifier' if patient_or_sample == 'patient' else '#Sample Identifier'

    # Initialize header with ID column
    header_rows = [
        {
            'column_name': id_column,
            'display_label': id_label,
            'datatype': 'STRING',
            'description': '1'
        }
    ]

    print(f"Initialized header with ID column: {id_column}")

    # Process each YAML config
    for idx, row in df_manifest.iterrows():
        summary_id = row['summary_id']
        yaml_path = row['yaml_config_path']

        print(f"\n[{idx + 1}/{len(df_manifest)}] Processing: {summary_id}")
        print(f"  YAML: {yaml_path}")

        try:
            # Load YAML config
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)

            # Get column metadata
            column_metadata = config.get('column_metadata', {})
            columns_list = config.get('columns', [])
            key_column = config.get('key_column', 'MRN')

            # Process each column (skip key column - it's replaced by ID)
            added_cols = 0
            for col in columns_list:
                # Skip key column
                if col == key_column:
                    continue

                # Get metadata
                metadata = column_metadata.get(col, {})

                # Extract or use defaults
                label = metadata.get('label', col)
                datatype = metadata.get('datatype', 'STRING')
                comment = metadata.get('comment', '')

                # Add to header rows
                header_rows.append({
                    'column_name': col.upper(),
                    'display_label': label,
                    'datatype': datatype,
                    'description': comment
                })
                added_cols += 1

            print(f"  Added {added_cols} column(s) to header")

        except Exception as e:
            print(f"  ✗ ERROR processing {summary_id}: {str(e)}")
            continue

    # Create DataFrame
    df_header = pd.DataFrame(header_rows)

    print(f"\n{'='*80}")
    print(f"HEADER CREATION COMPLETE")
    print(f"{'='*80}")
    print(f"Total columns: {len(df_header)}")

    # Reorder header rows to match merged data column order
    print(f"\nReordering header to match merged data column order...")
    merged_columns = list(df_merged_data.columns)
    print(f"  Merged data has {len(merged_columns)} columns")

    # Create ordered header based on merged data columns
    header_dict = {row['column_name']: row for row in header_rows}
    ordered_header_rows = []

    for col in merged_columns:
        if col in header_dict:
            ordered_header_rows.append(header_dict[col])
        else:
            # Column in merged data but not in header - add with defaults
            print(f"  ⚠ WARNING: Column '{col}' in data but not in YAML configs. Adding with defaults.")
            ordered_header_rows.append({
                'column_name': col,
                'display_label': col,
                'datatype': 'STRING',
                'description': ''
            })

    df_header_ordered = pd.DataFrame(ordered_header_rows)

    # Check for columns in header but not in data
    header_cols_set = set(df_header['column_name'])
    data_cols_set = set(merged_columns)
    missing_in_data = header_cols_set - data_cols_set

    if missing_in_data:
        print(f"  ⚠ WARNING: {len(missing_in_data)} column(s) in header but not in data:")
        for col in sorted(missing_in_data):
            print(f"    - {col}")

    print(f"\n✓ Header ordered to match data")
    print(f"  Final header rows: {len(df_header_ordered)}")

    return df_header_ordered


def save_header(
    df_header: pd.DataFrame,
    output_volume_path: str,
    output_catalog: str,
    output_schema: str,
    output_table: str,
    obj_db: DatabricksAPI
):
    """
    Save header to both Databricks volume and table.

    Parameters
    ----------
    df_header : pd.DataFrame
        Header in tall format
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
    print(f"SAVING HEADER")
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
    print(f"Format:      Tall (4 columns × {len(df_header)} rows)")

    # Save to both volume and table
    obj_db.write_db_obj(
        df=df_header,
        volume_path=output_volume_path,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    print(f"\n✓ Saved header:")
    print(f"  Volume: {output_volume_path}")
    print(f"  Table:  {output_catalog}.{output_schema}.{output_table}")
    print(f"  Shape:  {df_header.shape}")
    print(f"  Columns: {list(df_header.columns)}")


def main():
    parser = argparse.ArgumentParser(
        description="Create cBioPortal header from YAML configurations"
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
        "--merged_data_path",
        required=True,
        help="Databricks volume path to merged data from merge_intermediate_summaries.py"
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
        help="Output Databricks volume path for header"
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
        help="Table name for header"
    )

    args = parser.parse_args()

    print(f"\n{'#'*80}")
    print(f"# SUMMARY HEADER CREATOR")
    print(f"{'#'*80}")
    print(f"Manifest:            {args.manifest}")
    print(f"Merged data:         {args.merged_data_path}")
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

    # Load merged data (to get column order)
    print(f"Loading merged data: {args.merged_data_path}")
    df_merged_data = obj_db.read_db_obj(volume_path=args.merged_data_path, sep='\t')
    print(f"  Merged data shape: {df_merged_data.shape}")
    print(f"  Columns: {list(df_merged_data.columns)}\n")

    # Create header from YAML configs
    df_header = create_header_from_yamls(
        df_manifest=df_manifest,
        df_merged_data=df_merged_data,
        patient_or_sample=args.patient_or_sample
    )

    # Save header
    save_header(
        df_header=df_header,
        output_volume_path=args.output_volume_path,
        output_catalog=args.output_catalog,
        output_schema=args.output_schema,
        output_table=args.output_table,
        obj_db=obj_db
    )

    print(f"\n{'#'*80}")
    print(f"# HEADER CREATION COMPLETE")
    print(f"{'#'*80}")
    print(f"Output: {args.output_volume_path}")
    print(f"Table:  {args.output_catalog}.{args.output_schema}.{args.output_table}")
    print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()
