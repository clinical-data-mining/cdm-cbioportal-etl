import yaml
import os

import pandas as pd


class CbioportalUpdateConfig(object):
    def __init__(
            self,
            fname_yaml_config: str
    ):
        """
        Initialize the YamlParser object, which loads and processes a YAML configuration file.

        Args:
            fname_yaml_config: Path to the YAML configuration file.
        """
        # Codebook variables
        self._df_codebook_metadata = None
        self._df_codebook_table = None
        self._df_codebook_project = None

        # Load the YAML configuration file
        with open(fname_yaml_config, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)
        self._config = config
        self._load_codebook()

    def return_filename_codebook_metadata(self):
        # Load metadata sheet
        config = self._config
        # Codebook path
        path_codebook = config.get('codebook', {}).get('path')
        f = config.get('codebook', {}).get('fname_metadata')
        filename = os.path.join(path_codebook, f)
        return filename

    def return_filename_codebook_tables(self):
        # Load metadata sheet
        config = self._config
        # Codebook path
        path_codebook = config.get('codebook', {}).get('path')
        f = config.get('codebook', {}).get('fname_tables')
        filename = os.path.join(path_codebook, f)
        return filename

    def return_filename_codebook_projects(self):
        # Load metadata sheet
        config = self._config
        # Codebook path
        path_codebook = config.get('codebook', {}).get('path')
        f = config.get('codebook', {}).get('fname_project')
        filename = os.path.join(path_codebook, f)
        return filename

    def _load_codebook(self):
        """
        Load the codebook CSV files (metadata, table, project) as Pandas dataframes
        from the paths specified in the YAML configuration file.
        """
        # Load the YAML configuration file
        config = self._config

        filename = self.return_filename_codebook_metadata()
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_metadata:
            self._df_codebook_metadata = pd.read_csv(fname_codebook_metadata)

        # Load table sheet
        filename = self.return_filename_codebook_tables()
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_table:
            self._df_codebook_table = pd.read_csv(fname_codebook_table)

        # Load project sheet
        filename = self.return_filename_codebook_projects()
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_project:
            self._df_codebook_project = pd.read_csv(fname_codebook_project)

        return None

    def return_credential_filename(self):
        """
        Retrieve the credential filename from the YAML configuration.

        Returns:
            str: Path to the MinIO environment credential file.
        """
        config = self._config
        env_minio = config.get('inputs', {}).get('env_minio')
        return env_minio

    def return_credential_filename_databricks(self):
        """
        Retrieve the credential filename from the YAML configuration.

        Returns:
            str: Path to the MinIO environment credential file.
        """
        config = self._config
        env_minio = config.get('inputs_databricks', {}).get('fname_databricks_config')
        return env_minio

    def return_sample_list_filename(self):
        """
        Retrieve the sample list filename from the YAML configuration.

        Returns:
            str: Path to the sample ID file.
        """
        config = self._config
        fname_cbio_sid = config.get('inputs', {}).get('fname_cbio_sid')
        return fname_cbio_sid

    def return_sample_exclude_list(self):
        """
        Retrieve the filename of the sample exclusion list from the YAML configuration.

        Returns:
            str: Path to the sample exclusion file.
        """
        config = self._config
        fname_sample_remove = config.get('inputs', {}).get('fname_sample_remove')
        return fname_sample_remove

    def return_databricks_configs(self):
        config = self._config
        dict_databricks_configs = config.get('inputs_databricks', {})
        return dict_databricks_configs

    def return_manifest_filename_patient(self):
        """
        Retrieve the patient manifest filename from the YAML configuration.

        Returns:
            str: Path to the patient manifest file.
        """
        config = self._config
        fname_manifest_patient = config.get('inputs', {}).get('fname_manifest_patient')
        return fname_manifest_patient

    def return_manifest_filename_sample(self):
        """
        Retrieve the sample manifest filename from the YAML configuration.

        Returns:
            str: Path to the sample manifest file.
        """
        config = self._config
        fname_manifest_sample = config.get('inputs', {}).get('fname_manifest_sample')
        return fname_manifest_sample

    def return_intermediate_folder_path(self):
        """
        Retrieve the intermediate folder path for CBio summary files from the YAML configuration.

        Returns:
            str: Path to the intermediate summary folder on MinIO.
        """
        config = self._config
        path_minio_cbio_summary_intermediate = config.get('inputs', {}).get('path_minio_cbio_summary_intermediate')
        return path_minio_cbio_summary_intermediate

    def return_production_or_test_indicator(self):
        config = self._config
        production_or_test = config.get('inputs', {}).get('production_or_test')
        return production_or_test

    def return_template_info(self) -> dict:
        """
        Retrieve the template file paths for CBio summary files from the YAML configuration.

        Returns:
            dict: A dictionary with the template file paths.
        """
        # Load the YAML configuration file
        config = self._config

        # Access the filenames in the YAML file
        template_files = config.get('template_files', {})

        return template_files

    def return_dict_datahub_to_minio(self):
        """
        Map DataHub and MinIO filenames and return a dictionary of the mapped paths.

        Returns:
            dict: A dictionary with DataHub paths as keys and corresponding MinIO paths as values.
        """
        config = self._config
        list_timeline_files = list(self._df_codebook_table['cbio_deid_filename'].dropna())
        deid_filenames = list(config.get('deid_filenames', {}).values())
        list_deid_files = deid_filenames + list_timeline_files

        path_datahub = config.get('inputs', {}).get('path_datahub')
        path_minio = config.get('inputs', {}).get('path_minio_cbio')

        list_deid_files_datahub = [os.path.join(path_datahub,x) for x in list_deid_files]
        list_deid_files_minio = [os.path.join(path_minio,x) for x in list_deid_files]

        dict_datahub_to_minio = dict(zip(list_deid_files_datahub, list_deid_files_minio))

        return dict_datahub_to_minio

    def return_dict_phi_to_deid_timeline_production(self) -> dict:
        """
        Map template files to actual filenames using the 'template_files' section
        of the YAML file and the mapping from the loaded codebook table.

        Returns:
            dict: A dictionary mapping template keys to actual filenames from the codebook.
        """
        """
        Load filenames from a YAML configuration and map them to corresponding
        filenames from a JSON mapping file.

        Returns:
            dict: A dictionary with the mapped filenames.
        """
        config = self._config

        path_datahub = config.get('inputs', {}).get('path_datahub')
        codebook_table = self._df_codebook_table
        df_codebook_timeline_prod = codebook_table[codebook_table['cbio_timeline_file_production'] == 'x'].copy()
        df_timeline_files = df_codebook_timeline_prod[['cdm_source_table', 'cbio_deid_filename']].dropna()
        df_timeline_files['cbio_deid_filename'] = df_timeline_files['cbio_deid_filename'].apply(lambda x: os.path.join(path_datahub, x) )

        dict_phi_to_deid_timeline = dict(zip(list(df_timeline_files['cdm_source_table']), list(df_timeline_files['cbio_deid_filename'])))

        return dict_phi_to_deid_timeline

    def return_dict_phi_to_deid_timeline_testing(self) -> dict:
        """
        Map template files to actual filenames using the 'template_files' section
        of the YAML file and the mapping from the loaded codebook table. (Testing study)

        Returns:
            dict: A dictionary mapping template keys to actual filenames from the codebook.
        """
        """
        Load filenames from a YAML configuration and map them to corresponding
        filenames from a JSON mapping file.

        Returns:
            dict: A dictionary with the mapped filenames.
        """
        config = self._config

        path_datahub = config.get('inputs', {}).get('path_datahub')
        codebook_table = self._df_codebook_table
        df_codebook_timeline_prod = codebook_table[codebook_table['cbio_timeline_file_testing'] == 'x'].copy()
        df_timeline_files = df_codebook_timeline_prod[['cdm_source_table', 'cbio_deid_filename']].dropna()
        df_timeline_files['cbio_deid_filename'] = df_timeline_files['cbio_deid_filename'].apply(lambda x: os.path.join(path_datahub, x) )

        dict_phi_to_deid_timeline = dict(zip(list(df_timeline_files['cdm_source_table']), list(df_timeline_files['cbio_deid_filename'])))

        return dict_phi_to_deid_timeline

    def return_filenames_deid_datahub(self) -> dict:
        config = self._config

        path_datahub = config.get('deid_filenames', {})

        return path_datahub


