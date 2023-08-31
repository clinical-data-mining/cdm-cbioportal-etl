import pandas as pd
from constants import (
    FNAME_METADATA, 
    FNAME_PROJECT, 
    FNAME_TABLES
)


def init_metadata():
    # Load CDM Codebook files
    df_metadata = pd.read_csv(FNAME_METADATA)
    df_project = pd.read_csv(FNAME_PROJECT)
    df_tables = pd.read_csv(FNAME_TABLES)

    return df_metadata, df_tables, df_project