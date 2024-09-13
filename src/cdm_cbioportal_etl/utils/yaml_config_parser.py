import yaml
import os

import pandas as pd


class YamlParser(object):
    def __init__(
            self,
            fname_yaml_config: str
    ):
        """
        Load filenames from a YAML configuration and map them to corresponding
        filenames from a JSON mapping file.

        Args:
            config_path (str): Path to the YAML configuration file.
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

    def _load_codebook(self):
        # Load the YAML configuration file
        config = self._config

        # Codebook path
        path_codebook = config.get('codebook', {}).get('path')

        # Load metadata sheet
        f = config.get('codebook', {}).get('fname_metadata')
        filename = os.path.join(path_codebook, f)
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_metadata:
            self._df_codebook_metadata = pd.read_csv(fname_codebook_metadata)

        # Load table sheet
        f = config.get('codebook', {}).get('fname_tables')
        filename = os.path.join(path_codebook, f)
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_table:
            self._df_codebook_table = pd.read_csv(fname_codebook_table)

        # Load project sheet
        f = config.get('codebook', {}).get('fname_project')
        filename = os.path.join(path_codebook, f)
        # Load the JSON mapping file
        with open(filename, 'r') as fname_codebook_project:
            self._df_codebook_project = pd.read_csv(fname_codebook_project)

        return None


    def return_template_info(self) -> dict:
        """
        Load filenames from a YAML configuration and map them to corresponding
        filenames from a JSON mapping file.

        Returns:
            dict: A dictionary with the template paths and filenames.
        """
        # Load the YAML configuration file
        config = self._config

        # Access the filenames in the YAML file
        template_files = config.get('template_files', {})

        return template_files

    def return_filenames_deid_datahub(self) -> dict:
        config = self._config

        # Access the filenames in the YAML file
        deid_filenames = config.get('deid_filenames', {})
        path_datahub = config.get('inputs', {}).get('path_datahub')
        print(path_datahub)

        # Map the YAML keys to their corresponding actual filenames
        dict_deid_filenames_datahub = {}
        for key, deid_filename in deid_filenames.items():
            dict_deid_filenames_datahub[key] = os.path.join(path_datahub, deid_filename)

        return dict_deid_filenames_datahub

    def return_filenames_deid_minio(self) -> dict:
        config = self._config

        # Access the filenames in the YAML file
        deid_filenames = config.get('deid_filenames', {})
        path_minio = config.get('inputs', {}).get('path_minio_cbio')
        print(path_minio)

        # Map the YAML keys to their corresponding actual filenames
        dict_deid_filenames_minio = {}
        for key, deid_filename in deid_filenames.items():
            dict_deid_filenames_minio[key] = os.path.join(path_minio, deid_filename)

        return dict_deid_filenames_minio

    def return_dict_copy_to_minio(self):
        dict_deid_filenames_minio = self.return_filenames_deid_minio()
        dict_deid_filenames_deid_datahub = self.return_filenames_deid_datahub()

        # Combine the values from both dictionaries using the same keys
        combined_dict = {
            dict_deid_filenames_datahub[key]: dict_deid_filenames_minio[key]
            for key in dict_deid_filenames_datahub
            if key in dict_deid_filenames_minio
        }

        return combined_dict

    def return_mapped_filenames(self) -> dict:
        """
        Load filenames from a YAML configuration and map them to corresponding
        filenames from a JSON mapping file.

        Args:
            config_path (str): Path to the YAML configuration file.
            mapping_path (str): Path to the JSON mapping file.

        Returns:
            dict: A dictionary with the mapped filenames.
        """
        config = self._config
        mapping = self._df_codebook_table

        # Access the filenames in the YAML file
        template_files = config.get('template_files', {})

        # Map the YAML keys to their corresponding actual filenames
        mapped_filenames = {}
        for key, yaml_filename in template_files.items():
            actual_filename = mapping.get(key)
            if actual_filename:
                mapped_filenames[key] = actual_filename
            else:
                mapped_filenames[key] = yaml_filename  # If no mapping found, use the original

        return mapped_filenames
