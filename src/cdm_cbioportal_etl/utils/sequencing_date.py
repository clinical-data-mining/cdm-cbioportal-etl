"""
sequencing_date.py


"""
import pandas as pd

from msk_cdm.minio import MinioAPI


def date_of_sequencing(
        *,
        minio_env,
        fname_samples,
        fname_save_date_of_seq
):
    """

    :param minio_env: MinIO environment filename
    :param fname_samples: object path to the pathology report table
    :param fname_save_date_of_seq: object path to where date of sequencing is saved
    :return: df_path: dataframe with age at sequencing
    """

    # Load data
    ## Create Minio object
    obj_minio = MinioAPI(fname_minio_env=minio_env)

    ## Load pathology report table
    col_keep = ['DMP_ID', 'SAMPLE_ID', 'DTE_TUMOR_SEQUENCING']
    obj = obj_minio.load_obj(path_object=fname_samples)
    df_path1 = pd.read_csv(obj, sep='\t', usecols=col_keep, low_memory=False)
    df_path = df_path1.dropna()
    df_path = df_path.rename(
        columns={
            'DMP_ID': 'PATIENT_ID',
            'DTE_TUMOR_SEQUENCING': 'SEQ_DATE'
        }
    )
    # Save dataframe
    obj_minio.save_obj(
        df=df_path,
        path_object=fname_save_date_of_seq,
        sep='\t'
    )

    return df_path

