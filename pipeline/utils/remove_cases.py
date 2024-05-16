import pandas as pd

from msk_cdm.minio import MinioAPI
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm
from msk_cdm.data_processing import mrn_zero_pad


FNAME_MINIO_ENV = config_cdm.minio_env
FNAME_PATHOLOGY = config_cdm.fname_path_clean
fname_cbio_ids = '/gpfs/mindphidata/cdm_repos/datahub/impact-data/data_clinical_sample.txt'
COLS_PATHOLOGY = ['MRN', 'DTE_PATH_PROCEDURE', 'SAMPLE_ID', 'DMP_ID']


def load_pathology_data(
        fname_minio_env,
        fname_pathology
):
    # Init Minio and load pathology report table containing dmp ids and MRNs
    obj_minio = MinioAPI(fname_minio_env=fname_minio_env)

    print('Loading %s' % fname_pathology)
    obj = obj_minio.load_obj(path_object=fname_pathology)
    df_path = pd.read_csv(obj, header=0, low_memory=False, sep='\t', usecols=COLS_PATHOLOGY)

    df_path = df_path.dropna().copy()
    df_path['DTE_PATH_PROCEDURE'] = pd.to_datetime(
        df_path['DTE_PATH_PROCEDURE'],
        errors='coerce'
    )

    return df_path

def dmp_id_case_to_remove(
        fname_cbio_ids=fname_cbio_ids,
        fname_minio_env=FNAME_MINIO_ENV,
        fname_pathology=FNAME_PATHOLOGY,
):
    # Load currently available IDs
    df_ids = pd.read_csv(fname_cbio_ids, sep='\t')
    df_path = load_pathology_data(
        fname_minio_env=fname_minio_env,
        fname_pathology=fname_pathology
    )

    # Generate list of sample and patient IDs from cbioportal backend
    list_ids_p = list(set(df_ids['PATIENT_ID_x']))
    list_ids_s = list(set(df_ids['SAMPLE_ID']))

    df_path_filt = df_path[df_path['SAMPLE_ID'].notnull() & df_path['SAMPLE_ID'].str.contains('T')].copy()
    df_path_filt['DMP_ID_DERIVED'] = df_path_filt['SAMPLE_ID'].apply(lambda x: x[:9])

    # Remove cases where DMP_ID does not match DMP_ID Derived
    df_path_sample_id_error = df_path_filt[df_path_filt['DMP_ID_DERIVED'] != df_path_filt['DMP_ID']]
    df_path_filt1 = df_path_filt[df_path_filt['DMP_ID_DERIVED'] == df_path_filt['DMP_ID']]


    df_path_filt1 = df_path_filt1.sort_values(by=['MRN', 'DTE_PATH_PROCEDURE'])
    df_path_g = df_path_filt1.groupby(['MRN', 'DMP_ID'])['DTE_PATH_PROCEDURE'].first().reset_index()

    df_path_g = mrn_zero_pad(df=df_path_g, col_mrn='MRN')

    # Remove mismapped cases
    filt_mismapped = df_path_g['MRN'].duplicated(keep=False) | df_path_g['DMP_ID'].duplicated(keep=False)
    filt_multiple_dmp = (df_path_filt['DMP_ID_DERIVED'] != df_path_filt['DMP_ID']) & (df_path_filt['DMP_ID'] != 'P-0000000')

    list_dmp_id_error1 = list(df_path_g.loc[filt_mismapped, 'DMP_ID'])
    list_dmp_id_error2 = list(df_path_filt.loc[filt_multiple_dmp, 'DMP_ID'])

    list_dmp_id_error = list(set(list_dmp_id_error1 + list_dmp_id_error2))

    return list_dmp_id_error


def main():
    list_dmp_id_error = dmp_id_case_to_remove()

    print(list_dmp_id_error)

if __name__ == '__main__':
    main()




