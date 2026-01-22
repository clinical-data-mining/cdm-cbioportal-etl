"""
cbioportal_formatting_radrpt_tumor_sites_summary.py

Processing script to create patient-level summary of tumor sites from the timeline table
produced by cbioportal_formatting_radrpt_tumor_sites.py.

This aggregates tumor site presence at the patient level (one row per MRN).

Usage:
    python cbioportal_formatting_radrpt_tumor_sites_summary.py \
        --table_path "catalog.schema.tumor_sites_timeline_table" \
        --databricks_env "/path/to/databricks_env" \
        --output_table "catalog.schema.tumor_sites_summary_table" \
        --volume_path "/Volumes/catalog/schema/volume_name/output_file.tsv"
"""
import argparse
import pandas as pd

from msk_cdm.data_processing import mrn_zero_pad
from msk_cdm.databricks import DatabricksAPI


class TumorSitesSummaryProcessor(object):
    def __init__(self, table_path, fname_databricks_config, output_table, volume_path):
        self._table_path = table_path
        self._output_table = output_table
        self._volume_path = volume_path
        self._databricks_env = fname_databricks_config

        # Objects
        self._obj_databricks = None

        # Data frames
        ## Input dataframes
        self._df_input = None

        ## Summary df
        self._df_summary = None

        self._init_process()

    def _init_process(self):
        # Init Databricks
        obj_databricks = DatabricksAPI(fname_databricks_env=self._databricks_env)
        self._obj_databricks = obj_databricks

        # Load data files
        self._load_data()

        # Create patient-level summary
        df_summary = self._create_summary()
        self._df_summary = df_summary

        if self._output_table is not None:
            print('Saving to Databricks table: %s' % self._output_table)
            print('Using volume path: %s' % self._volume_path)

            # Write DataFrame to Databricks volume
            self._obj_databricks.write_db_obj(
                df=self._df_summary,
                volume_path=self._volume_path,
                sep='\t',
                overwrite=True
            )
            print('Successfully wrote to volume')

            # Parse output table into catalog, schema, table components
            parts = self._output_table.split('.')
            if len(parts) != 3:
                raise ValueError(f"Output table must be in format catalog.schema.table, got: {self._output_table}")
            catalog, schema, table = parts

            # Create table from volume
            dict_database_table_info = {
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'volume_path': self._volume_path,
                'sep': '\t'
            }

            self._obj_databricks.create_table_from_volume(
                dict_database_table_info=dict_database_table_info
            )
            print('Successfully created table from volume')

    def _load_data(self):
        ## Load tumor sites timeline table from Databricks
        table_name = self._table_path
        print('Loading %s' % table_name)
        sql = f"SELECT * FROM {table_name}"
        df1 = self._obj_databricks.query_from_sql(sql=sql)
        df = mrn_zero_pad(df=df1, col_mrn='MRN')

        self._df_input = df

        return None

    def return_summary(self):
        return self._df_summary

    def _normalize_column_name(self, col_name):
        """
        Convert tumor site column names to uppercase with underscores.
        Maps specific tumor site names to their standardized format.
        """
        if col_name == 'MRN':
            return col_name

        # Mapping of tumor site names to standardized column names
        column_mapping = {
            'Adrenal Glands': 'ADRENAL_GLANDS',
            'Bone': 'BONE',
            'CNS/Brain': 'CNS_BRAIN',
            'Intra-Abdominal': 'INTRA_ABDOMINAL',
            'Liver': 'LIVER',
            'Lung': 'LUNG',
            'Lymph Nodes': 'LYMPH_NODES',
            'Other': 'OTHER',
            'Pleura': 'PLEURA',
            'Reproductive Organs': 'REPRODUCTIVE_ORGANS'
        }

        # Return mapped name if it exists, otherwise apply generic transformation
        if col_name in column_mapping:
            return column_mapping[col_name]
        else:
            # Fallback: replace spaces, slashes, and hyphens with underscores, then uppercase
            normalized = col_name.replace(' ', '_').replace('/', '_').replace('-', '_')
            return normalized.upper()

    def _create_summary(self):
        df_input = self._df_input.copy()

        # Filter out "No Tumor Sites" and null tumor sites
        df_filtered = df_input[
            (df_input['TUMOR_SITE'] != 'No Tumor Sites') &
            (df_input['TUMOR_SITE'].notnull())
        ].copy()

        if len(df_filtered) == 0:
            print("Warning: No tumor sites found after filtering")
            return pd.DataFrame(columns=['MRN'])

        # Create a pivot table: MRN x TUMOR_SITE with presence indicator
        # If a patient has ANY record with a tumor site, mark as present
        df_pivot = df_filtered.groupby(['MRN', 'TUMOR_SITE']).size().unstack(fill_value=0)

        # Convert counts to binary (any presence > 0)
        df_pivot = (df_pivot > 0).astype(int)

        # Convert to Yes/No format
        df_summary = df_pivot.replace({1: 'Yes', 0: 'No'}).reset_index()

        print(f"Created summary for {len(df_summary)} patients")
        print(f"Original tumor site columns: {list(df_summary.columns[1:])}")

        # Normalize column names to uppercase with underscores
        df_summary.columns = [self._normalize_column_name(col) for col in df_summary.columns]

        print(f"Normalized tumor site columns: {list(df_summary.columns[1:])}")

        return df_summary


def main():
    parser = argparse.ArgumentParser(
        description='Create patient-level tumor sites summary from timeline table'
    )
    parser.add_argument(
        '--table_path',
        type=str,
        required=False,
        help='Databricks input table path - output from cbioportal_formatting_radrpt_tumor_sites.py (e.g., catalog.schema.table_name)'
    )
    parser.add_argument(
        '--databricks_env',
        type=str,
        required=True,
        help='Path to Databricks environment file'
    )
    parser.add_argument(
        '--output_table',
        type=str,
        required=True,
        help='Databricks output table name (e.g., catalog.schema.output_table_name)'
    )
    parser.add_argument(
        '--volume_path',
        type=str,
        required=True,
        help='Databricks volume path (e.g., /Volumes/catalog/schema/volume_name/file.tsv)'
    )

    args = parser.parse_args()

    obj_processor = TumorSitesSummaryProcessor(
        table_path=args.table_path,
        fname_databricks_config=args.databricks_env,
        output_table=args.output_table,
        volume_path=args.volume_path
    )

    df = obj_processor.return_summary()
    print(f"\nSummary table shape: {df.shape}")
    print("\nFirst few rows:")
    print(df.head())


if __name__ == '__main__':
    main()