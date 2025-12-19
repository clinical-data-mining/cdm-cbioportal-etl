"""
summary_merger.py

Merges multiple intermediate summary files into a single cBioPortal summary file.

This module provides simplified functionality for merging intermediate files
created by SummaryConfigProcessor. It handles:
- Loading intermediate files (with headers)
- Merging headers horizontally
- Merging data horizontally
- Combining header and data into final cBioPortal format
"""
import pandas as pd
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

        # Load template
        self.df_template_header, self.df_template_data = self._load_template()

        # Storage for merged results
        self.df_merged_header = None
        self.df_merged_data = None
        self.df_final = None

    def _load_template(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load template file and split into header and data.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            (header_df, data_df)
        """
        print(f"Loading template: {self.fname_template}")

        df_full = self.obj_db.read_db_obj(volume_path=self.fname_template, sep='\t')

        # Split into header and data
        df_header = df_full.iloc[:NROWS_HEADER].copy()
        df_data = df_full.iloc[NROWS_HEADER:].reset_index(drop=True).copy()

        # Ensure data is string type
        df_data = df_data.astype(str)

        print(f"  Template loaded: {df_data.shape[0]} rows, {df_data.shape[1]} columns")

        # Initialize merged with template (just the ID column)
        self.df_merged_header = df_header[[self.id_column]].copy()
        self.df_merged_data = df_data[[self.id_column]].copy()

        return df_header, df_data

    def load_intermediate_file(
        self,
        fname_intermediate: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load an intermediate file and split into header and data.

        Parameters
        ----------
        fname_intermediate : str
            Path to intermediate file

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            (header_df, data_df)
        """
        print(f"Loading intermediate file: {fname_intermediate}")

        df_full = self.obj_db.read_db_obj(volume_path=fname_intermediate, sep='\t')

        # Split into header and data
        df_header = df_full.iloc[:NROWS_HEADER].copy()
        df_data = df_full.iloc[NROWS_HEADER:].reset_index(drop=True).copy()

        # Ensure data is string type
        df_data = df_data.astype(str)

        print(f"  Loaded: {df_data.shape[0]} rows, {df_data.shape[1]} columns")

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

    def merge_all_intermediates(self, intermediate_files: List[str]):
        """
        Merge all intermediate files in order.

        Parameters
        ----------
        intermediate_files : List[str]
            List of paths to intermediate files to merge
        """
        print(f"\n{'='*80}")
        print(f"Merging {len(intermediate_files)} intermediate files")
        print(f"{'='*80}\n")

        for idx, fname in enumerate(intermediate_files, 1):
            print(f"[{idx}/{len(intermediate_files)}] {fname}")
            df_header, df_data = self.load_intermediate_file(fname)
            self.merge_intermediate(df_header, df_data)
            print()

        print(f"✓ All intermediate files merged")
        print(f"  Final shape: {self.df_merged_data.shape}")

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
