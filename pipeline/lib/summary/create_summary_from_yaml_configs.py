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
        config_dir='config/summaries',
        production_or_test='production',
        cohort='mskimpact'
    )

    # Process all summaries
    obj.create_summaries_and_headers(
        patient_or_sample='patient',
        table_template='/path/to/template.txt'
    )
"""
import os
import glob
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

from msk_cdm.databricks import DatabricksAPI
from msk_cdm.data_processing import set_debug_console, mrn_zero_pad

from .summary_config_processor import SummaryConfigProcessor
from ..utils import constants

set_debug_console()

COL_SUMMARY_FNAME_SAVE = constants.COL_SUMMARY_FNAME_SAVE
COL_SUMMARY_HEADER_FNAME_SAVE = constants.COL_SUMMARY_HEADER_FNAME_SAVE
COL_RPT_NAME = constants.COL_RPT_NAME
COL_ANCHOR_DATE = constants.COL_ANCHOR_DATE
NROWS_HEADER = 4

# Anchor dates table location
TABLE_ANCHOR_DATES = 'cdsi_eng_phi.cdm_eng_cbioportal_etl.timeline_anchor_dates'


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
        production_or_test : str
            'production' or 'test' - determines which source tables to use
        cohort : str
            Cohort name (e.g., 'mskimpact', 'mskaccess')
        """
        self._fname_databricks_env = fname_databricks_env
        self._config_dir = config_dir
        self._production_or_test = production_or_test
        self._cohort = cohort

        # DataFrames
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
        print(f'Loading anchor dates: {TABLE_ANCHOR_DATES}')
        sql = f"SELECT * FROM {TABLE_ANCHOR_DATES}"
        self._df_anchor = self._obj_db.query_from_sql(sql=sql)

        # Zero-pad MRN
        self._df_anchor = mrn_zero_pad(df=self._df_anchor, col_mrn='MRN')

        # Convert DATE_TUMOR_SEQUENCING to datetime
        self._df_anchor[COL_ANCHOR_DATE] = pd.to_datetime(
            self._df_anchor[COL_ANCHOR_DATE], errors='coerce'
        )

        print(f'  Loaded {self._df_anchor.shape[0]} anchor dates')

        return None

    def return_anchor_dates(self):
        """Return the loaded anchor dates dataframe."""
        return self._df_anchor

    def _load_template(self, table_template: str, patient_or_sample: str = None) -> pd.DataFrame:
        """
        Load template table and extract data portion.

        For patient summaries, extracts only PATIENT_ID column.
        For sample summaries, extracts only SAMPLE_ID column.

        Parameters
        ----------
        table_template : str
            Table name, volume path, or local file path to template
        patient_or_sample : str, optional
            'patient' or 'sample' to determine which ID column to keep

        Returns
        -------
        pd.DataFrame
            Template data with only the relevant ID column (deduplicated)
        """
        print(f'Loading template: {table_template}')

        # Check if it's a local file, Databricks volume path, or table name
        if os.path.exists(table_template):
            # Local file
            print(f'  Loading from local file system')
            df_full = pd.read_csv(table_template, sep='\t', dtype=str)
        elif table_template.startswith('/Volumes'):
            # Databricks volume path
            print(f'  Loading from Databricks volume')
            df_full = self._obj_db.read_db_obj(volume_path=table_template, sep='\t')
        else:
            # Databricks table
            print(f'  Loading from Databricks table')
            sql = f"SELECT * FROM {table_template}"
            df_full = self._obj_db.query_from_sql(sql=sql)

        print(f'  Template shape: {df_full.shape}')

        # Template has standard CSV format with 1 header row
        # Row 0: Column names (SAMPLE_ID, PATIENT_ID, etc.)
        # Rows 1+: Data
        df_template = df_full.copy()

        # Ensure all columns are string type
        df_template = df_template.astype(str)

        print(f'  Template columns: {list(df_template.columns)}')

        # Subset to relevant ID column and drop duplicates
        if patient_or_sample:
            id_column = 'PATIENT_ID' if patient_or_sample == 'patient' else 'SAMPLE_ID'

            if id_column in df_template.columns:
                print(f'  Subsetting to {id_column} column only')
                df_template = df_template[[id_column]].copy()

                # Drop duplicates
                original_rows = df_template.shape[0]
                df_template = df_template.drop_duplicates()
                deduplicated_rows = df_template.shape[0]

                if original_rows != deduplicated_rows:
                    print(f'  Dropped {original_rows - deduplicated_rows} duplicate rows')

                print(f'  Final template shape: {df_template.shape}')
            else:
                raise ValueError(f"Template does not have {id_column} column. Available: {list(df_template.columns)}")

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

    def _should_process_yaml(
        self,
        config: Dict,
        patient_or_sample: str
    ) -> bool:
        """
        Check if a YAML config should be processed.

        Parameters
        ----------
        config : Dict
            Loaded YAML configuration
        patient_or_sample : str
            'patient' or 'sample'

        Returns
        -------
        bool
            True if should process, False to skip
        """
        # Check 1: Has the right source table for production/test mode?
        if self._production_or_test == 'production':
            if not config.get('source_table_prod'):
                print(f"  Skipping: no source_table_prod")
                return False
        else:  # test
            if not config.get('source_table_dev'):
                print(f"  Skipping: no source_table_dev")
                return False

        # Check 2: Matches patient/sample level?
        if config.get('patient_or_sample') != patient_or_sample:
            print(f"  Skipping: this is a {config.get('patient_or_sample')} summary")
            return False

        return True

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
            Summary info if successful, None if skipped/failed
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

            # Check if should process
            if not self._should_process_yaml(processor.config, patient_or_sample):
                return None

            # Process the summary
            df_data = processor.process_summary(
                df_anchor=df_anchor,
                df_template=df_template
            )

            # Save intermediate file
            volume_path = processor.save_intermediate(
                df_data=df_data,
                save_to_table=save_to_table
            )

            # Return info for tracking
            summary_info = {
                'summary_id': processor.config['summary_id'],
                'yaml_file': yaml_file,
                'intermediate_path': volume_path,
                'config': processor.config  # Need for header creation later
            }

            print(f"✓ Successfully processed: {processor.config['summary_id']}")

            return summary_info

        except Exception as e:
            print(f"✗ ERROR processing {os.path.basename(yaml_file)}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def create_summaries_and_headers(
        self,
        patient_or_sample: str,
        table_template: str,
        save_to_table: bool = False
    ) -> List[Dict]:
        """
        Create cBioPortal summary intermediate files for all YAML configs.

        This processes all summaries and returns info about what was processed:
        1. Load template
        2. Auto-discover YAML configs with appropriate source tables
        3. Process each config (filter by patient/sample level and source table availability)
        4. Create intermediate data files (no headers)
        5. Return list of processed summaries for downstream merging

        Parameters
        ----------
        patient_or_sample : str
            'patient' or 'sample'
        table_template : str
            Template table name or volume path
        save_to_table : bool
            Whether to save intermediate files to Databricks tables

        Returns
        -------
        List[Dict]
            List of processed summary info dicts, each containing:
            - summary_id
            - yaml_file
            - intermediate_path
            - config (YAML config dict, needed for header creation)
        """
        print(f"\n{'='*80}")
        print(f"CREATING {patient_or_sample.upper()} SUMMARIES")
        print(f"{'='*80}\n")

        # Load anchor dates
        df_anchor = self._df_anchor

        # Load template (subset to relevant ID column and deduplicate)
        df_template = self._load_template(table_template, patient_or_sample)

        # Get all YAML files
        yaml_files = self._get_yaml_files(patient_or_sample)

        # Process each YAML file
        print(f"\n{'='*80}")
        print(f"PROCESSING SUMMARIES")
        print(f"{'='*80}")

        processed_summaries = []
        skipped_count = 0

        for yaml_file in yaml_files:
            summary_info = self.process_single_summary(
                yaml_file=yaml_file,
                df_anchor=df_anchor,
                df_template=df_template,
                patient_or_sample=patient_or_sample,
                save_to_table=save_to_table
            )

            if summary_info is None:
                skipped_count += 1
            else:
                processed_summaries.append(summary_info)

        # Print summary
        print(f"\n{'='*80}")
        print(f"SUMMARY PROCESSING COMPLETE")
        print(f"{'='*80}")
        print(f"Processed: {len(processed_summaries)}")
        print(f"Skipped: {skipped_count}")

        if processed_summaries:
            print(f"\nProcessed summaries:")
            for summary in processed_summaries:
                print(f"  ✓ {summary['summary_id']}: {summary['intermediate_path']}")
        else:
            print(f"\nWARNING: No summaries were processed for {patient_or_sample}")

        return processed_summaries
