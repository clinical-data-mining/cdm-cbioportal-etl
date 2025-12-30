"""
codebook_to_yaml_converter.py

Utility script to convert CDM codebook CSV files into YAML configuration files
for the new summary config-based processing pipeline.

This reads the existing:
- CDM-Codebook - metadata.csv
- CDM-Codebook - tables.csv

And generates individual YAML files for each summary in pipeline/config/summaries/
"""
import os
import pandas as pd
import yaml
from pathlib import Path


def convert_codebook_to_yaml_configs(
    fname_metadata: str,
    fname_tables: str,
    output_dir: str,
    default_catalog_prod: str,
    default_schema_prod: str,
    default_volume_prod: str,
    default_catalog_dev: str,
    default_schema_dev: str,
    default_volume_dev: str
):
    """
    Convert codebook CSV files to YAML configuration files.

    Parameters
    ----------
    fname_metadata : str
        Path to metadata CSV file
    fname_tables : str
        Path to tables CSV file
    output_dir : str
        Directory to write YAML files
    default_catalog_prod : str
        Default catalog for production destination
    default_schema_prod : str
        Default schema for production destination
    default_volume_prod : str
        Default volume for production destination
    default_catalog_dev : str
        Default catalog for dev destination
    default_schema_dev : str
        Default schema for dev destination
    default_volume_dev : str
        Default volume for dev destination
    """
    # Read codebooks
    print("Loading codebook files...")
    df_metadata = pd.read_csv(fname_metadata)
    df_tables = pd.read_csv(fname_tables)

    # Filter metadata to rows marked for either cbioportal or test portal
    # This ensures YAML files work for both production and test modes
    df_metadata_filtered = df_metadata[
        (df_metadata['for_cbioportal'] == 'x') | (df_metadata['for_test_portal'] == 'x')
    ].copy()

    print(f"Found {df_metadata_filtered.shape[0]} fields marked for cbioportal")

    # Get unique form names
    form_names = df_metadata_filtered['form_name'].unique()
    print(f"Found {len(form_names)} unique forms/tables")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process each form
    for form_name in form_names:
        print(f"\n{'='*80}")
        print(f"Processing form: {form_name}")
        print(f"{'='*80}")

        # Get table info for this form
        table_info = df_tables[df_tables['form_name'] == form_name]

        if table_info.empty:
            print(f"  WARNING: No table info found for form '{form_name}', skipping")
            continue

        # Use first row if multiple entries
        table_row = table_info.iloc[0]

        # Determine if patient or sample level
        if pd.notna(table_row['cbio_summary_id_sample']) and table_row['cbio_summary_id_sample']:
            patient_or_sample = 'sample'
            key_column = table_row['cbio_summary_id_sample']
        elif pd.notna(table_row['cbio_summary_id_patient']) and table_row['cbio_summary_id_patient']:
            patient_or_sample = 'patient'
            key_column = table_row['cbio_summary_id_patient']
        else:
            print(f"  WARNING: No patient or sample ID column found for '{form_name}', skipping")
            continue

        # Get metadata for this form
        form_metadata = df_metadata_filtered[df_metadata_filtered['form_name'] == form_name]

        if form_metadata.empty:
            print(f"  WARNING: No metadata found for form '{form_name}', skipping")
            continue

        # Build source table paths - always include both prod and dev
        source_table_prod = table_row.get('cdm_source_table', '')
        source_table_dev = table_row.get('cdm_source_table_dev', '')

        # If dev is not specified, use prod table as fallback
        if not source_table_dev:
            source_table_dev = source_table_prod

        # Handle cases where source table is a relative path
        if source_table_prod and not source_table_prod.startswith('cdsi_'):
            # Assume it needs catalog/schema prefix
            source_table_prod = f"{default_catalog_prod}.cdm_impact_pipeline_prod.{source_table_prod.split('/')[-1].replace('.tsv', '')}"
        if source_table_dev and not source_table_dev.startswith('cdsi_'):
            source_table_dev = f"{default_catalog_dev}.cdm_impact_pipeline_dev.{source_table_dev.split('/')[-1].replace('.tsv', '')}"

        # Get columns for this form
        columns = []
        date_columns = []
        column_metadata = {}

        # Add key column first
        if key_column not in form_metadata['field_name'].values:
            columns.append(key_column)

        for _, row in form_metadata.iterrows():
            field_name = row['field_name']
            columns.append(field_name)

            # Check if it's a date column
            if pd.notna(row.get('text_validation_type_or_sh')) and row['text_validation_type_or_sh'] == 'date_mdy':
                date_columns.append(field_name)

            # Build metadata for this column
            field_label = row.get('field_label', field_name)
            field_type = row.get('field_type', 'STRING')

            # Map field_type to cBioPortal datatype
            if field_type == 'INT' or field_type == 'FLOAT':
                datatype = 'NUMBER'
            else:
                datatype = 'STRING'

            # Build comment
            field_note = row.get('field_note', '')
            missing_reason = row.get('reasons_for_missing_data', '')
            source = row.get('souce_from_idb_or_cdm', '')

            comment_parts = []
            if field_note:
                comment_parts.append(f"---DESCRIPTION: {field_note}")
            if missing_reason:
                comment_parts.append(f"---MISSING DATA: {missing_reason}")
            if source:
                comment_parts.append(f"---SOURCE: {source}")

            comment = f"{field_label} " + " ".join(comment_parts) if comment_parts else field_label

            # Get fill value
            fill_value = row.get('fill_value', 'NA')
            if pd.isna(fill_value) or fill_value == '':
                fill_value = 'NA'

            column_metadata[field_name] = {
                'label': field_label,
                'datatype': datatype,
                'comment': comment,
                'fill_value': str(fill_value)
            }

        # Create YAML config
        summary_id = form_name.lower().replace(' ', '_').replace('-', '_')
        filename = f"{summary_id}_summary.tsv"

        config = {
            'summary_id': summary_id,
            'patient_or_sample': patient_or_sample,
            'source_table_prod': source_table_prod,
            'source_table_dev': source_table_dev,
            'key_column': key_column,
            'columns': columns,
            'date_columns': date_columns,
            'dest_prod': {
                'catalog': default_catalog_prod,
                'schema': default_schema_prod,
                'volume_name': default_volume_prod,
                'filename': filename
            },
            'dest_dev': {
                'catalog': default_catalog_dev,
                'schema': default_schema_dev,
                'volume_name': default_volume_dev,
                'filename': filename
            },
            'column_metadata': column_metadata
        }

        # Write YAML file
        output_filename = os.path.join(output_dir, f"{summary_id}.yaml")

        print(f"  Writing: {output_filename}")
        print(f"    Patient/Sample: {patient_or_sample}")
        print(f"    Key column: {key_column}")
        print(f"    Columns: {len(columns)}")
        print(f"    Date columns: {len(date_columns)}")
        print(f"    Source (prod): {source_table_prod}")
        print(f"    Source (dev):  {source_table_dev}")

        with open(output_filename, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print(f"  ✓ Created: {output_filename}")

    print(f"\n{'='*80}")
    print(f"YAML Conversion Complete")
    print(f"{'='*80}")
    print(f"Generated {len(form_names)} YAML configuration files in: {output_dir}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert CDM codebook CSV files to YAML configuration files"
    )
    parser.add_argument(
        '--metadata',
        required=True,
        help='Path to metadata CSV file (CDM-Codebook - metadata.csv)'
    )
    parser.add_argument(
        '--tables',
        required=True,
        help='Path to tables CSV file (CDM-Codebook - tables.csv)'
    )
    parser.add_argument(
        '--output_dir',
        required=True,
        help='Directory to write YAML files'
    )
    parser.add_argument(
        '--catalog_prod',
        default='cdsi_eng_phi',
        help='Production catalog name'
    )
    parser.add_argument(
        '--schema_prod',
        default='cdm_eng_cbioportal_etl',
        help='Production schema name'
    )
    parser.add_argument(
        '--volume_prod',
        default='cdm_eng_cbioportal_etl_volume',
        help='Production volume name'
    )
    parser.add_argument(
        '--catalog_dev',
        default='cdsi_eng_phi',
        help='Dev catalog name'
    )
    parser.add_argument(
        '--schema_dev',
        default='cdm_eng_cbioportal_etl',
        help='Dev schema name'
    )
    parser.add_argument(
        '--volume_dev',
        default='cdm_eng_cbioportal_etl_volume_dev',
        help='Dev volume name'
    )

    args = parser.parse_args()

    convert_codebook_to_yaml_configs(
        fname_metadata=args.metadata,
        fname_tables=args.tables,
        output_dir=args.output_dir,
        default_catalog_prod=args.catalog_prod,
        default_schema_prod=args.schema_prod,
        default_volume_prod=args.volume_prod,
        default_catalog_dev=args.catalog_dev,
        default_schema_dev=args.schema_dev,
        default_volume_dev=args.volume_dev
    )
