import yaml
import pandas as pd

def load_mapped_filenames(config_path: str) -> dict:
    """
    Load filenames from a YAML configuration and map them to corresponding
    filenames from a JSON mapping file.

    Args:
        config_path (str): Path to the YAML configuration file.
        mapping_path (str): Path to the JSON mapping file.

    Returns:
        dict: A dictionary with the mapped filenames.
    """
    # Load the YAML configuration file
    with open(config_path, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    mapping_path = config.get('codebook', 'fname_tables')
    # Load the JSON mapping file
    with open(mapping_path, 'r') as fname_codebook_table:
        mapping = pd.read_csv(fname_codebook_table)

    # Access the filenames in the YAML file
    yaml_filenames = config.get('filenames', {})

    # Map the YAML keys to their corresponding actual filenames
    mapped_filenames = {}
    for key, yaml_filename in yaml_filenames.items():
        actual_filename = mapping.get(key)
        if actual_filename:
            mapped_filenames[key] = actual_filename
        else:
            mapped_filenames[key] = yaml_filename  # If no mapping found, use the original

    return mapped_filenames
