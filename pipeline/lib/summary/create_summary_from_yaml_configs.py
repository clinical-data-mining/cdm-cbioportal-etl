"""
create_summary_from_yaml_configs.py

Class-based module for creating cBioPortal summary files from YAML configurations.

This replaces the codebook-based approach with a YAML config-based approach:
- Load individual YAML files for each summary
- Process summaries one at a time or all at once
- Create intermediate files with headers
- Generate manifest files
- Support both patient and sample level summaries

Usage:
    obj = YamlConfigToCbioportalFormat(
        fname_databricks_env=fname_databricks_env,
        config_dir='pipeline/config/summaries',
        volume_path_summary_intermediate='/Volumes/.../intermediate_files',
        production_or_test='production',
        cohort='mskimpact'
    )

    # Process all summaries
    obj.create_summaries_and_headers(
        patient_or_sample='patient',
        fname_manifest='/Volumes/.../manifest_patient.csv',
        table_template='cdsi_prod.schema.patient_template'
    )
"""
import os
import glob
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import set_debug_console

from .summary_config_processor import SummaryConfigProcessor
from ..utils.get_anchor_dates import get_anchor_dates
from ..utils import constants

set_debug_console()

COL_SUMMARY_FNAME_SAVE = constants.COL_SUMMARY_FNAME_SAVE
COL_SUMMARY_HEADER_FNAME_SAVE = constants.COL_SUMMARY_HEADER_FNAME_SAVE
COL_RPT_NAME = constants.COL_RPT_NAME
NROWS_HEADER = 4


class YamlConfigToCbioportalFormat(object):
    """
    Process YAML configuration files to create cBioPortal summary files.

    This class provides a structured approach to:
    1. Load YAML configs for each summary
    2. Process summaries individually
    3. Create intermediate files with headers
    4. Generate manifest files for downstream merging
    """

    def __init__(
        self,
        fname_databricks_env: str,
        config_dir: str,
        volume_path_summary_intermediate: str,
        production_or_test: str = 'production',
        cohort: str = 'mskimpact'
    ):
        """
        Initialize the YAML config processor.

        Parameters
        ----------
        fname_databricks_env : str
            Path to Databricks environment file
        config_dir : str
            Directory containing YAML configuration files
        volume_path_summary_intermediate : str
            Base path for intermediate files
        production_or_test : str
            'production' or 'test' - determines which source tables to use
        cohort : str
            Cohort name (e.g., 'mskimpact', 'mskaccess')
        """
        self._fname_databricks_env = fname_databricks_env
        self._config_dir = config_dir
        self._volume_path_summary_intermediate = volume_path_summary_intermediate
        self._production_or_test = production_or_test
        self._cohort = cohort

        # DataFrames
        self._df_manifest = None
        self._df_anchor = None

        # Initialize
        self._init()

    def _init(self):
        """Initialize Databricks connection and load anchor dates."""
        # Create Databricks API object
        self._obj_db = DatabricksAPI(
            fname_databricks_env=self._fname_databricks_env
        )

        # Load anchor dates for deidentification
        print('Loading anchor dates...')
        self._df_anchor = get_anchor_dates(self._fname_databricks_env)
        print(f'  Loaded {self._df_anchor.shape[0]} anchor dates')

        return None

    def return_anchor_dates(self):
        """Return the loaded anchor dates dataframe."""
        return self._df_anchor

    def return_manifest(self):
        """Return the manifest dataframe."""
        return self._df_manifest

    def _load_template(self, table_template: str) -> pd.DataFrame:
        """
        Load template table and extract data portion.

        Parameters
        ----------
        table_template : str
            Table name or volume path to template

        Returns
        -------
        pd.DataFrame
            Template data (without header rows)
        """
        print(f'Loading template: {table_template}')

        # Check if it's a table name or volume path
        if table_template.startswith('/Volumes'):
            df_full = self._obj_db.read_db_obj(volume_path=table_template, sep='\t')
        else:
            sql = f"SELECT * FROM {table_template}"
            df_full = self._obj_db.query_from_sql(sql=sql)

        print(f'  Template shape (with header): {df_full.shape}')

        # Remove first NROWS_HEADER rows to get data only
        df_template = df_full.iloc[NROWS_HEADER:].reset_index(drop=True)
        df_template = df_template.astype(str)

        print(f'  Template shape (data only): {df_template.shape}')

        return df_template

    def _get_yaml_files(self, patient_or_sample: str) -> List[str]:
        """
        Get all YAML files in config directory.

        Parameters
        ----------
        patient_or_sample : str
            'patient' or 'sample' - filter will be applied during processing

        Returns
        -------
        List[str]
            List of YAML file paths
        """
        yaml_files = glob.glob(os.path.join(self._config_dir, '*.yaml'))
        print(f'Found {len(yaml_files)} YAML configuration files in {self._config_dir}')
        return yaml_files

    def process_single_summary(
        self,
        yaml_file: str,
        df_anchor: pd.DataFrame,
        df_template: pd.DataFrame,
        patient_or_sample: str,
        save_to_table: bool = False
    ) -> Optional[Dict]:
        """
        Process a single summary from YAML config.

        Parameters
        ----------
        yaml_file : str
            Path to YAML configuration file
        df_anchor : pd.DataFrame
            Anchor dates dataframe
        df_template : pd.DataFrame
            Template dataframe
        patient_or_sample : str
            'patient' or 'sample'
        save_to_table : bool
            Whether to save to Databricks table

        Returns
        -------
        Optional[Dict]
            Manifest entry if successful, None if skipped/failed
        """
        print(f"\n{'-'*80}")
        print(f"Processing: {os.path.basename(yaml_file)}")
        print(f"{'-'*80}")

        try:
            # Create processor
            processor = SummaryConfigProcessor(
                fname_yaml_config=yaml_file,
                fname_databricks_env=self._fname_databricks_env,
                production_or_test=self._production_or_test,
                cohort=self._cohort
            )

            # Check if this config matches the patient/sample level we're processing
            if processor.config['patient_or_sample'] != patient_or_sample:
                print(f"  Skipping (this is a {processor.config['patient_or_sample']} summary)")
                return None

            # Process the summary
            df_header, df_data = processor.process_summary(
                df_anchor=df_anchor,
                df_template=df_template
            )

            # Save intermediate file
            volume_path = processor.save_intermediate(
                df_header=df_header,
                df_data=df_data,
                save_to_table=save_to_table
            )

            # Get manifest entry
            manifest_entry = processor.get_manifest_entry()
            manifest_entry['data_path'] = volume_path

            print(f"✓ Successfully processed: {processor.config['summary_id']}")

            return manifest_entry

        except Exception as e:
            print(f"✗ ERROR processing {os.path.basename(yaml_file)}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def create_summaries_and_headers(
        self,
        patient_or_sample: str,
        fname_manifest: str,
        table_template: str,
        save_to_table: bool = False
    ):
        """
        Create cBioPortal summary and header files for all YAML configs.

        This is the main method that processes all summaries:
        1. Load template
        2. Find all YAML configs
        3. Process each config (filter by patient/sample level)
        4. Create intermediate files with headers
        5. Generate manifest file

        Parameters
        ----------
        patient_or_sample : str
            'patient' or 'sample'
        fname_manifest : str
            Path to save manifest file
        table_template : str
            Template table name or volume path
        save_to_table : bool
            Whether to save intermediate files to Databricks tables
        """
        print(f"\n{'='*80}")
        print(f"CREATING {patient_or_sample.upper()} SUMMARIES")
        print(f"{'='*80}\n")

        # Initialize manifest
        self.summary_manifest_init()

        # Load anchor dates
        df_anchor = self._df_anchor

        # Load template
        df_template = self._load_template(table_template)

        # Get all YAML files
        yaml_files = self._get_yaml_files(patient_or_sample)

        # Process each YAML file
        print(f"\n{'='*80}")
        print(f"PROCESSING SUMMARIES")
        print(f"{'='*80}")

        processed_count = 0
        skipped_count = 0
        failed_count = 0

        for yaml_file in yaml_files:
            manifest_entry = self.process_single_summary(
                yaml_file=yaml_file,
                df_anchor=df_anchor,
                df_template=df_template,
                patient_or_sample=patient_or_sample,
                save_to_table=save_to_table
            )

            if manifest_entry is None:
                skipped_count += 1
            else:
                # Add to manifest
                self.summary_manifest_append(
                    summary_id=manifest_entry['summary_id'],
                    fname_data=manifest_entry['data_path']
                )
                processed_count += 1

        # Save manifest
        print(f"\n{'='*80}")
        print(f"SUMMARY PROCESSING COMPLETE")
        print(f"{'='*80}")
        print(f"Processed: {processed_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Failed: {failed_count}")

        if processed_count > 0:
            df_manifest = self.return_manifest()
            print(f"\nManifest contains {df_manifest.shape[0]} entries:")
            print(df_manifest)

            # Save manifest
            self.summary_manifest_save(fname_save=fname_manifest)
        else:
            print(f"\nWARNING: No summaries were processed for {patient_or_sample}")

        return None

    def summary_manifest_init(self):
        """Initialize empty manifest dataframe."""
        cols_manifest = [
            COL_RPT_NAME,
            COL_SUMMARY_FNAME_SAVE
        ]
        df_manifest = pd.DataFrame(columns=cols_manifest)
        self._df_manifest = df_manifest
        return None

    def summary_manifest_append(
        self,
        summary_id: str,
        fname_data: str
    ):
        """
        Append entry to manifest.

        Parameters
        ----------
        summary_id : str
            Summary identifier
        fname_data : str
            Path to data file
        """
        df_manifest = self._df_manifest
        dict_append = {
            COL_RPT_NAME: summary_id,
            COL_SUMMARY_FNAME_SAVE: fname_data
        }

        new_row_df = pd.DataFrame.from_dict(dict_append, orient='index').T
        df_manifest = pd.concat([df_manifest, new_row_df], ignore_index=True)
        self._df_manifest = df_manifest

        return None

    def summary_manifest_save(self, fname_save: str):
        """
        Save manifest to file.

        Parameters
        ----------
        fname_save : str
            Path to save manifest
        """
        print(f'\nSaving manifest: {fname_save}')
        self._obj_db.write_db_obj(
            df=self._df_manifest,
            volume_path=fname_save,
            sep=',',
            overwrite=True
        )
        print(f'✓ Manifest saved')

        return None
