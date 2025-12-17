"""
sequencing_date.py

Extracts sequencing date from pathology table and saves to Databricks
"""
import pandas as pd

from msk_cdm.databricks import DatabricksAPI


def date_of_sequencing(
        *,
        databricks_env,
        table_samples,
        volume_path_save_date_of_seq,
        table_save_date_of_seq=None,
        catalog=None,
        schema=None
):
    """

    :param databricks_env: Databricks environment filename
    :param table_samples: Full table name for pathology report table (e.g., 'catalog.schema.table')
    :param volume_path_save_date_of_seq: Volume path where date of sequencing is saved
    :param table_save_date_of_seq: Optional table name to create from the data
    :param catalog: Optional catalog name for table creation
    :param schema: Optional schema name for table creation
    :return: df_path: dataframe with sequencing dates
    """

    # Load data
    ## Create Databricks object
    obj_db = DatabricksAPI(fname_databricks_env=databricks_env)

    ## Load pathology report table
    col_keep = ['DMP_ID', 'SAMPLE_ID', 'DTE_TUMOR_SEQUENCING']
    cols_str = ', '.join(col_keep)
    sql = f"SELECT {cols_str} FROM {table_samples}"
    df_path1 = obj_db.query_from_sql(sql=sql)
    df_path = df_path1.dropna()
    df_path = df_path.rename(
        columns={
            'DMP_ID': 'PATIENT_ID',
            'DTE_TUMOR_SEQUENCING': 'SEQ_DATE'
        }
    )

    # Save dataframe to Databricks
    # Prepare table info dictionary if table name provided
    dict_database_table_info = None
    if table_save_date_of_seq and catalog and schema:
        dict_database_table_info = {
            'catalog': catalog,
            'schema': schema,
            'table': table_save_date_of_seq,
            'volume_path': volume_path_save_date_of_seq,
            'sep': '\t'
        }
        print(f'Creating table: {catalog}.{schema}.{table_save_date_of_seq}')

    obj_db.write_db_obj(
        df=df_path,
        volume_path=volume_path_save_date_of_seq,
        sep='\t',
        overwrite=True,
        dict_database_table_info=dict_database_table_info
    )

    return df_path

