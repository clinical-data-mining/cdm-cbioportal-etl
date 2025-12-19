"""
summary_config_processor.py

Processes a single summary file from a YAML configuration.
This module provides a clean, modular approach to creating cBioPortal summary files.

Process:
1. Load YAML configuration containing table source, columns, and metadata
2. Load and subset source table data
3. Merge with anchor dates for deidentification
4. Convert date columns to intervals
5. Merge with template to ensure all patients/samples are included
6. Backfill missing data with specified fill values
7. Save intermediate file with header information
"""
import os
import yaml
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import mrn_zero_pad


class SummaryConfigProcessor:
    """
    Processes a single summary file based on YAML configuration.

    This class handles the complete workflow for creating one intermediate
    summary file, including data loading, deidentification, and formatting.
    """

    def __init__(
        self,
        fname_yaml_config: str,
        fname_databricks_env: str,
        production_or_test: str = 'production',
        cohort: str = 'mskimpact'
    ):
        """
        Initialize the summary processor.

        Parameters
        ----------
        fname_yaml_config : str
            Path to YAML configuration file for this summary
        fname_databricks_env : str
            Path to Databricks environment file
        production_or_test : str, optional
            'production' or 'test' to determine which tables/destinations to use
        cohort : str, optional
            Cohort name for volume path construction (e.g., 'mskimpact', 'mskaccess')
        """
        self.fname_yaml_config = fname_yaml_config
        self.fname_databricks_env = fname_databricks_env
        self.production_or_test = production_or_test
        self.cohort = cohort

        # Load configuration
        self.config = self._load_config()

        # Initialize Databricks API
        self.obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

        # Determine source table and destination
        self.source_table = self._get_source_table()
        self.dest_config = self._get_dest_config()
        self.volume_path = self._build_volume_path()

        # Will be set during processing
        self.template_id_column = None

    def _load_config(self) -> Dict:
        """Load YAML configuration file."""
        with open(self.fname_yaml_config, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def _get_source_table(self) -> str:
        """Get the appropriate source table based on production/test setting."""
        if self.production_or_test == 'production':
            return self.config['source_table_prod']
        else:
            return self.config['source_table_dev']

    def _get_dest_config(self) -> Dict:
        """Get the appropriate destination config based on production/test setting."""
        if self.production_or_test == 'production':
            return self.config['dest_prod']
        else:
            return self.config['dest_dev']

    def _build_volume_path(self) -> str:
        """
        Build the full Databricks volume path for the intermediate file.

        Returns
        -------
        str
            Full volume path like /Volumes/{catalog}/{schema}/{volume_name}/cbioportal/intermediate_files/{cohort}/{filename}
        """
        dest = self.dest_config
        volume_path = (
            f"/Volumes/{dest['catalog']}/{dest['schema']}/{dest['volume_name']}/"
            f"cbioportal/intermediate_files/{self.cohort}/{dest['filename']}"
        )
        return volume_path

    def process_summary(
        self,
        df_anchor: pd.DataFrame,
        df_template: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Process the summary file through the complete pipeline.

        Parameters
        ----------
        df_anchor : pd.DataFrame
            Anchor dates dataframe with columns: MRN, DMP_ID, DATE_TUMOR_SEQUENCING
        df_template : pd.DataFrame
            Template dataframe with PATIENT_ID or SAMPLE_ID column

        Returns
        -------
        pd.DataFrame
            Processed data with standard column names
        """
        print(f"\n{'='*80}")
        print(f"Processing summary: {self.config['summary_id']}")
        print(f"{'='*80}")

        # Step 1: Load and subset data
        df_data = self._load_and_subset_data()

        # Step 2: Merge with anchor dates and deidentify
        df_merged = self._merge_with_anchor_dates(df_data, df_anchor)

        # Step 3: Convert date columns to intervals
        df_with_intervals = self._convert_dates_to_intervals(df_merged, df_anchor)

        # Step 4: Merge with template
        df_final = self._merge_with_template(df_with_intervals, df_template)

        # Step 5: Backfill missing data
        df_backfilled = self._backfill_missing_data(df_final)

        print(f"✓ Summary processing complete: {self.config['summary_id']}")
        print(f"  Shape: {df_backfilled.shape}")

        return df_backfilled

    def _load_and_subset_data(self) -> pd.DataFrame:
        """Load source table and subset to specified columns."""
        print(f"Loading source table: {self.source_table}")

        columns = self.config['columns']
        columns_str = ', '.join(columns)
        sql = f"SELECT {columns_str} FROM {self.source_table}"

        df = self.obj_db.query_from_sql(sql=sql)
        print(f"  Loaded {df.shape[0]} rows, {df.shape[1]} columns")

        return df

    def _merge_with_anchor_dates(
        self,
        df_data: pd.DataFrame,
        df_anchor: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge data with anchor dates for deidentification.

        Parameters
        ----------
        df_data : pd.DataFrame
            Source data
        df_anchor : pd.DataFrame
            Anchor dates with MRN, DMP_ID, DATE_TUMOR_SEQUENCING

        Returns
        -------
        pd.DataFrame
            Data merged with anchor dates, MRN replaced with DMP_ID
        """
        print("Merging with anchor dates for deidentification")

        key_column = self.config['key_column']

        # Handle MRN key - zero pad and merge
        if key_column == 'MRN':
            df_data = mrn_zero_pad(df=df_data, col_mrn='MRN')
            df_anchor = mrn_zero_pad(df=df_anchor, col_mrn='MRN')
            df_merged = df_anchor.merge(right=df_data, how='inner', on='MRN')
            df_merged = df_merged.drop(columns=['MRN'])
        else:
            # For other keys (like SAMPLE_ID), merge on DMP_ID
            df_anchor_subset = df_anchor[['DMP_ID', 'DATE_TUMOR_SEQUENCING']].drop_duplicates()
            df_merged = df_anchor_subset.merge(
                right=df_data,
                how='inner',
                left_on='DMP_ID',
                right_on=key_column
            )

        print(f"  After merge: {df_merged.shape[0]} rows")
        return df_merged

    def _convert_dates_to_intervals(
        self,
        df_data: pd.DataFrame,
        df_anchor: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Convert date columns to intervals (days from anchor date).

        Parameters
        ----------
        df_data : pd.DataFrame
            Data with date columns
        df_anchor : pd.DataFrame
            Anchor dates dataframe

        Returns
        -------
        pd.DataFrame
            Data with dates converted to day intervals
        """
        date_columns = self.config.get('date_columns', [])

        if not date_columns:
            print("No date columns to convert")
            return df_data

        print(f"Converting {len(date_columns)} date columns to intervals")

        # Ensure anchor date is datetime
        if 'DATE_TUMOR_SEQUENCING' in df_data.columns:
            df_data['DATE_TUMOR_SEQUENCING'] = pd.to_datetime(
                df_data['DATE_TUMOR_SEQUENCING'], errors='coerce'
            )

        # Convert each date column
        for col in date_columns:
            if col in df_data.columns:
                print(f"  Converting {col}")
                df_data[col] = pd.to_datetime(df_data[col], errors='coerce')
                df_data[col] = (df_data[col] - df_data['DATE_TUMOR_SEQUENCING']).dt.days

        # Drop anchor date column
        if 'DATE_TUMOR_SEQUENCING' in df_data.columns:
            df_data = df_data.drop(columns=['DATE_TUMOR_SEQUENCING'])

        return df_data

    def _merge_with_template(
        self,
        df_data: pd.DataFrame,
        df_template: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge with template to ensure all patients/samples are included.

        Parameters
        ----------
        df_data : pd.DataFrame
            Processed data
        df_template : pd.DataFrame
            Template with all PATIENT_ID or SAMPLE_ID values

        Returns
        -------
        pd.DataFrame
            Data merged with template (left join from template)
        """
        print("Merging with template")

        patient_or_sample = self.config['patient_or_sample']

        # Detect the ID column in the template
        # Templates may have DMP_ID, PATIENT_ID, or SAMPLE_ID
        template_columns = df_template.columns.tolist()

        if patient_or_sample == 'patient':
            # Check for patient ID columns in order of preference
            if 'PATIENT_ID' in template_columns:
                id_column_template = 'PATIENT_ID'
            elif 'DMP_ID' in template_columns:
                id_column_template = 'DMP_ID'
            else:
                raise ValueError(f"Template does not have PATIENT_ID or DMP_ID column. Columns: {template_columns}")
        else:
            # Sample level
            if 'SAMPLE_ID' in template_columns:
                id_column_template = 'SAMPLE_ID'
            elif 'DMP_ID' in template_columns:
                id_column_template = 'DMP_ID'
            else:
                raise ValueError(f"Template does not have SAMPLE_ID or DMP_ID column. Columns: {template_columns}")

        print(f"  Template ID column: {id_column_template}")

        # Store the template's ID column for use in header creation
        self.template_id_column = id_column_template

        # Rename DMP_ID in data to match template's ID column
        if 'DMP_ID' in df_data.columns and id_column_template != 'DMP_ID':
            df_data = df_data.rename(columns={'DMP_ID': id_column_template})
        elif 'DMP_ID' not in df_data.columns and id_column_template == 'DMP_ID':
            # Data doesn't have DMP_ID, keep as is
            pass

        # Upper case all column names
        df_data.columns = [x.upper().replace(' ', '_') for x in df_data.columns]

        # Merge with template
        print(f"  Template shape: {df_template.shape}")
        print(f"  Data shape: {df_data.shape}")

        df_merged = df_template.merge(
            right=df_data,
            how='left',
            on=id_column_template
        )

        # Rename to standard cBioPortal column names if needed
        standard_id_column = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'
        if id_column_template != standard_id_column:
            print(f"  Renaming {id_column_template} -> {standard_id_column} for cBioPortal format")
            df_merged = df_merged.rename(columns={id_column_template: standard_id_column})
            # Update the stored template ID column to the standard name
            self.template_id_column = standard_id_column

        print(f"  Merged shape: {df_merged.shape}")
        return df_merged

    def _backfill_missing_data(self, df_data: pd.DataFrame) -> pd.DataFrame:
        """
        Backfill missing data with specified fill values from metadata.

        Parameters
        ----------
        df_data : pd.DataFrame
            Data with potential missing values

        Returns
        -------
        pd.DataFrame
            Data with missing values filled
        """
        print("Backfilling missing data")

        column_metadata = self.config.get('column_metadata', {})

        for col in df_data.columns:
            col_upper = col.upper()

            # Find metadata for this column
            metadata = None
            for key in column_metadata.keys():
                if key.upper() == col_upper:
                    metadata = column_metadata[key]
                    break

            if metadata and 'fill_value' in metadata:
                fill_value = metadata['fill_value']
                df_data[col] = df_data[col].fillna(fill_value)
                print(f"  {col}: filled with '{fill_value}'")

        return df_data


    def save_intermediate(
        self,
        df_data: pd.DataFrame,
        save_to_table: bool = False
    ) -> str:
        """
        Save the intermediate file to Databricks volume.
        Saves just the data with column names (no header metadata rows).

        Parameters
        ----------
        df_data : pd.DataFrame
            Data dataframe with column names
        save_to_table : bool, optional
            Whether to also save to a Databricks table

        Returns
        -------
        str
            Path where the file was saved
        """
        print(f"Saving intermediate file: {self.volume_path}")

        # Prepare table info if needed
        dict_database_table_info = None
        if save_to_table:
            dest = self.dest_config
            table_name = dest['filename'].replace('.tsv', '').replace('.txt', '')
            dict_database_table_info = {
                'catalog': dest['catalog'],
                'schema': dest['schema'],
                'table': table_name,
                'volume_path': self.volume_path,
                'sep': '\t'
            }

        # Save to volume (just data, no header rows)
        self.obj_db.write_db_obj(
            df=df_data,
            volume_path=self.volume_path,
            sep='\t',
            overwrite=True,
            dict_database_table_info=dict_database_table_info
        )

        print(f"✓ Saved: {self.volume_path}")
        return self.volume_path

    def get_manifest_entry(self) -> Dict[str, str]:
        """
        Get manifest entry for this summary file.

        Returns
        -------
        Dict[str, str]
            Manifest entry with summary_id, data_path, header metadata
        """
        return {
            'summary_id': self.config['summary_id'],
            'data_path': self.volume_path,
            'patient_or_sample': self.config['patient_or_sample']
        }
