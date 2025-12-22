"""
summary_merger.py

Merges multiple intermediate summary files into a single cBioPortal summary file.

This module provides simplified functionality for merging intermediate files
created by SummaryConfigProcessor. It handles:
- Loading intermediate data files (no headers in intermediates)
- Merging data horizontally
- Creating header from YAML configs
- Combining header and data into final cBioPortal format
"""
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict

from msk_cdm.databricks import DatabricksAPI


NROWS_HEADER = 4
ROW_LABEL = 0
ROW_DATATYPE = 1
ROW_COMMENT = 2
ROW_HEADING = 3


class SummaryMerger:
    """
    Merges multiple intermediate summary files into a final cBioPortal summary.

    This class simplifies the merging process by removing interactive features
    and focusing on straightforward horizontal concatenation of intermediate files.
    """

    def __init__(
        self,
        fname_databricks_env: str,
        fname_template: str,
        patient_or_sample: str
    ):
        """
        Initialize the summary merger.

        Parameters
        ----------
        fname_databricks_env : str
            Path to Databricks environment file
        fname_template : str
            Path to template file (used as base for ID column)
        patient_or_sample : str
            'patient' or 'sample' to determine ID column type
        """
        self.fname_databricks_env = fname_databricks_env
        self.fname_template = fname_template
        self.patient_or_sample = patient_or_sample

        # Determine ID column
        self.id_column = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'
        self.id_label = '#Patient Identifier' if patient_or_sample == 'patient' else '#Sample Identifier'

        # Initialize Databricks API
        self.obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

        # Load template and initialize merged structures
        df_template = self._load_template()

        # Initialize merged data with template (just the ID column)
        self.df_merged_data = df_template.copy()

        # Initialize merged header with ID column header
        df_header = pd.DataFrame({
            0: [self.id_label, 'STRING', '1', self.id_column]
        })
        self.df_merged_header = df_header

        # Storage for final result
        self.df_final = None

    def _load_template(self) -> pd.DataFrame:
        """
        Load template file (standard CSV format with 1 header row).

        Returns
        -------
        pd.DataFrame
            Template data with ID column only
        """
        print(f"Loading template: {self.fname_template}")

        # Check if it's a local file or Databricks path
        if os.path.exists(self.fname_template):
            df_template = pd.read_csv(self.fname_template, sep='\t', dtype=str)
        else:
            df_template = self.obj_db.read_db_obj(volume_path=self.fname_template, sep='\t')

        # Ensure data is string type
        df_template = df_template.astype(str)

        # Extract only the ID column
        if self.id_column in df_template.columns:
            df_template = df_template[[self.id_column]].copy()

            # Drop duplicates
            original_rows = df_template.shape[0]
            df_template = df_template.drop_duplicates()

            if original_rows != df_template.shape[0]:
                print(f"  Dropped {original_rows - df_template.shape[0]} duplicate rows")
        else:
            raise ValueError(f"Template does not have {self.id_column} column. Available: {list(df_template.columns)}")

        print(f"  Template loaded: {df_template.shape[0]} unique {self.id_column} values")

        return df_template

    def _create_header_from_config(self, config: Dict) -> pd.DataFrame:
        """
        Create header dataframe from YAML configuration.

        Parameters
        ----------
        config : Dict
            YAML configuration dictionary

        Returns
        -------
        pd.DataFrame
            Header dataframe with 4 rows (label, datatype, comment, heading)
        """
        column_metadata = config.get('column_metadata', {})
        columns_list = config.get('columns', [])

        # Build header rows for each column (excluding key column which becomes ID)
        key_column = config.get('key_column', 'MRN')

        headers_dict = {}
        col_idx = 0

        for col in columns_list:
            # Skip key column (it's replaced by ID column)
            if col == key_column:
                continue

            # Get metadata for this column
            metadata = column_metadata.get(col, {})

            label = metadata.get('label', col)
            datatype = metadata.get('datatype', 'STRING')
            comment = metadata.get('comment', '')
            heading = col.upper().replace(' ', '_')

            headers_dict[col_idx] = [label, datatype, comment, heading]
            col_idx += 1

        # Create header dataframe
        df_header = pd.DataFrame(headers_dict)

        return df_header

    def load_intermediate_file(
        self,
        fname_intermediate: str,
        config: Dict
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load an intermediate file (data only) and create header from config.

        Parameters
        ----------
        fname_intermediate : str
            Path to intermediate file (data only, no headers)
        config : Dict
            YAML configuration dictionary for this summary

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            (header_df, data_df)
        """
        print(f"Loading intermediate file: {fname_intermediate}")

        # Load data (no headers in intermediate files)
        df_data = self.obj_db.read_db_obj(volume_path=fname_intermediate, sep='\t')

        # Ensure data is string type
        df_data = df_data.astype(str)

        print(f"  Loaded: {df_data.shape[0]} rows, {df_data.shape[1]} columns")

        # Create header from YAML config
        df_header = self._create_header_from_config(config)

        return df_header, df_data

    def merge_intermediate(
        self,
        df_header: pd.DataFrame,
        df_data: pd.DataFrame
    ):
        """
        Merge an intermediate file into the accumulated merged data.

        This performs horizontal concatenation, adding new columns while
        preserving the ID column alignment.

        Parameters
        ----------
        df_header : pd.DataFrame
            Header dataframe from intermediate file
        df_data : pd.DataFrame
            Data dataframe from intermediate file
        """
        print("Merging intermediate file into accumulated summary")

        # Remove ID column from new data (we already have it)
        columns_to_add_header = [col for col in df_header.columns if col != self.id_column]
        columns_to_add_data = [col for col in df_data.columns if col != self.id_column]

        if not columns_to_add_data:
            print("  No new columns to add (only ID column present)")
            return

        # Filter to only new columns
        df_header_new = df_header[columns_to_add_header].copy()
        df_data_new = df_data[columns_to_add_data].copy()

        # Check for duplicate columns and drop from current merged
        duplicate_cols = [col for col in columns_to_add_data if col in self.df_merged_data.columns]
        if duplicate_cols:
            print(f"  Replacing {len(duplicate_cols)} existing columns: {duplicate_cols}")
            self.df_merged_data = self.df_merged_data.drop(columns=duplicate_cols)
            # Also drop from header
            self.df_merged_header = self.df_merged_header.drop(columns=duplicate_cols)

        # Merge data (simple merge on ID)
        self.df_merged_data = self.df_merged_data.merge(
            right=df_data[[self.id_column] + columns_to_add_data],
            how='left',
            on=self.id_column
        )

        # Concatenate headers horizontally
        self.df_merged_header = pd.concat(
            [self.df_merged_header, df_header_new],
            axis=1,
            sort=False
        )

        print(f"  Merged summary now has {self.df_merged_data.shape[1]} columns")

    def merge_all_intermediates(self, processed_summaries: List[Dict]):
        """
        Merge all intermediate files in order.

        Parameters
        ----------
        processed_summaries : List[Dict]
            List of summary info dicts from YamlConfigToCbioportalFormat.create_summaries_and_headers()
            Each dict should contain:
            - 'summary_id': summary identifier
            - 'intermediate_path': path to intermediate file
            - 'config': YAML configuration dict
        """
        print(f"\n{'='*80}")
        print(f"Merging {len(processed_summaries)} intermediate files")
        print(f"{'='*80}\n")

        merged_count = 0
        skipped_count = 0

        for idx, summary_info in enumerate(processed_summaries, 1):
            summary_id = summary_info['summary_id']
            fname = summary_info['intermediate_path']
            config = summary_info['config']

            print(f"[{idx}/{len(processed_summaries)}] {summary_id}")
            print(f"  File: {fname}")

            try:
                df_header, df_data = self.load_intermediate_file(fname, config)
                self.merge_intermediate(df_header, df_data)
                merged_count += 1
                print(f"  ✓ Merged successfully")
            except Exception as e:
                print(f"  ✗ ERROR: Failed to load or merge file: {str(e)}")
                skipped_count += 1

            print()

        print(f"{'='*80}")
        print(f"MERGE SUMMARY")
        print(f"{'='*80}")
        print(f"Successfully merged: {merged_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Final shape: {self.df_merged_data.shape}")
        print(f"{'='*80}")

    def create_final_summary(self) -> pd.DataFrame:
        """
        Create the final summary by combining header and data.

        Returns
        -------
        pd.DataFrame
            Final cBioPortal-formatted summary with header rows at top
        """
        print("Creating final summary file")

        # Get column names from header (heading row)
        header_columns = list(self.df_merged_header.iloc[ROW_HEADING, :])

        # Rename data columns to match sequential column names from header
        # (header has sequential 0, 1, 2... column names, data has actual names)
        col_name_mapping = dict(zip(self.df_merged_data.columns, header_columns))
        df_data_renamed = self.df_merged_data.rename(columns=col_name_mapping)

        # Rename header columns to match (for concat)
        col_name_new = list(range(len(header_columns)))
        self.df_merged_header.columns = col_name_new
        df_data_renamed.columns = col_name_new

        # Combine header and data
        self.df_final = pd.concat(
            [self.df_merged_header, df_data_renamed],
            axis=0,
            ignore_index=True
        )

        print(f"✓ Final summary created: {self.df_final.shape}")

        return self.df_final

    def save_final_summary(
        self,
        fname_output: str,
        save_to_table: bool = False,
        catalog: str = None,
        schema: str = None,
        table_name: str = None
    ):
        """
        Save the final summary to a file.

        Parameters
        ----------
        fname_output : str
            Path to save the final summary file
        save_to_table : bool, optional
            Whether to also save to a Databricks table
        catalog : str, optional
            Catalog name for table (if save_to_table=True)
        schema : str, optional
            Schema name for table (if save_to_table=True)
        table_name : str, optional
            Table name (if save_to_table=True)
        """
        if self.df_final is None:
            raise ValueError("Final summary not created yet. Call create_final_summary() first.")

        print(f"Saving final summary: {fname_output}")

        # Prepare table info if needed
        dict_database_table_info = None
        if save_to_table:
            if not all([catalog, schema, table_name]):
                raise ValueError("catalog, schema, and table_name required when save_to_table=True")

            dict_database_table_info = {
                'catalog': catalog,
                'schema': schema,
                'table': table_name,
                'volume_path': fname_output,
                'sep': '\t'
            }

        # Save to volume
        self.obj_db.write_db_obj(
            df=self.df_final,
            volume_path=fname_output,
            sep='\t',
            overwrite=True,
            dict_database_table_info=dict_database_table_info
        )

        print(f"✓ Saved: {fname_output}")

    def get_merged_summary(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Get the current merged header and data (before final combination).

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            (header_df, data_df)
        """
        return self.df_merged_header, self.df_merged_data

    def get_final_summary(self) -> pd.DataFrame:
        """
        Get the final combined summary.

        Returns
        -------
        pd.DataFrame
            Final summary dataframe
        """
        if self.df_final is None:
            raise ValueError("Final summary not created yet. Call create_final_summary() first.")

        return self.df_final


def merge_summaries_from_manifest(
    fname_databricks_env: str,
    fname_manifest: str,
    fname_template: str,
    fname_output: str,
    patient_or_sample: str,
    save_to_table: bool = False,
    catalog: str = None,
    schema: str = None,
    table_name: str = None
) -> pd.DataFrame:
    """
    Convenience function to merge summaries from a manifest file.

    Parameters
    ----------
    fname_databricks_env : str
        Path to Databricks environment file
    fname_manifest : str
        Path to manifest CSV file with 'data_path' column
    fname_template : str
        Path to template file
    fname_output : str
        Path to save final summary
    patient_or_sample : str
        'patient' or 'sample'
    save_to_table : bool, optional
        Whether to save to a Databricks table
    catalog : str, optional
        Catalog for table (if save_to_table=True)
    schema : str, optional
        Schema for table (if save_to_table=True)
    table_name : str, optional
        Table name (if save_to_table=True)

    Returns
    -------
    pd.DataFrame
        Final merged summary
    """
    # Load manifest
    obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)
    df_manifest = obj_db.read_db_obj(volume_path=fname_manifest, sep=',')

    # Get list of intermediate files
    intermediate_files = df_manifest['data_path'].tolist()

    # Create merger
    merger = SummaryMerger(
        fname_databricks_env=fname_databricks_env,
        fname_template=fname_template,
        patient_or_sample=patient_or_sample
    )

    # Merge all intermediates
    merger.merge_all_intermediates(intermediate_files)

    # Create final summary
    df_final = merger.create_final_summary()

    # Save
    merger.save_final_summary(
        fname_output=fname_output,
        save_to_table=save_to_table,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

    return df_final


def merge_summaries_from_yaml_configs(
    fname_databricks_env: str,
    processed_summaries: List[Dict],
    fname_template: str,
    fname_output: str,
    patient_or_sample: str,
    save_to_table: bool = False,
    catalog: str = None,
    schema: str = None,
    table_name: str = None
) -> pd.DataFrame:
    """
    Convenience function to merge summaries from YAML config processing.

    This function takes the output from YamlConfigToCbioportalFormat.create_summaries_and_headers()
    and merges all intermediate files into a final cBioPortal summary.

    Parameters
    ----------
    fname_databricks_env : str
        Path to Databricks environment file
    processed_summaries : List[Dict]
        List of summary info dicts from create_summaries_and_headers()
        Each dict contains: summary_id, intermediate_path, config
    fname_template : str
        Path to template file
    fname_output : str
        Path to save final summary
    patient_or_sample : str
        'patient' or 'sample'
    save_to_table : bool, optional
        Whether to save to a Databricks table
    catalog : str, optional
        Catalog for table (if save_to_table=True)
    schema : str, optional
        Schema for table (if save_to_table=True)
    table_name : str, optional
        Table name (if save_to_table=True)

    Returns
    -------
    pd.DataFrame
        Final merged summary

    Example
    -------
    >>> from pipeline.lib.summary import YamlConfigToCbioportalFormat, merge_summaries_from_yaml_configs
    >>>
    >>> # Create intermediate files
    >>> obj = YamlConfigToCbioportalFormat(...)
    >>> processed_summaries = obj.create_summaries_and_headers(
    ...     patient_or_sample='patient',
    ...     table_template='/path/to/template.txt'
    ... )
    >>>
    >>> # Merge all intermediates
    >>> df_final = merge_summaries_from_yaml_configs(
    ...     fname_databricks_env='/path/to/env',
    ...     processed_summaries=processed_summaries,
    ...     fname_template='/path/to/template.txt',
    ...     fname_output='/Volumes/.../final_summary.txt',
    ...     patient_or_sample='patient'
    ... )
    """
    print(f"\n{'='*80}")
    print(f"MERGING SUMMARIES")
    print(f"{'='*80}\n")

    # Create merger
    merger = SummaryMerger(
        fname_databricks_env=fname_databricks_env,
        fname_template=fname_template,
        patient_or_sample=patient_or_sample
    )

    # Merge all intermediates
    merger.merge_all_intermediates(processed_summaries)

    # Create final summary
    df_final = merger.create_final_summary()

    # Save
    merger.save_final_summary(
        fname_output=fname_output,
        save_to_table=save_to_table,
        catalog=catalog,
        schema=schema,
        table_name=table_name
    )

    print(f"\n{'='*80}")
    print(f"MERGE COMPLETE")
    print(f"{'='*80}")
    print(f"Output: {fname_output}")
    print(f"Shape: {df_final.shape}")
    print(f"{'='*80}\n")

    return df_final
