import os
from dataclasses import dataclass
from msk_cdm.data_classes.legacy import CDMProcessingVariables as config_cdm


@dataclass
class CDMProcessingVariablesCbioportal:
    i = 0
    ##############################################################################
    # cBioPortal setup files
    ##############################################################################
    # For IMPACT transition with cbioportal backend team
    # path_datahub: str = "/gpfs/mindphidata/cdm_repos/datahub/cdm-data/"
    # fname_cbio_header_template_p: str = (
    #     "cbioportal/cbioportal_summary_header_patient.tsv"
    # )
    # fname_cbio_header_template_s: str = (
    #     "cbioportal/cbioportal_summary_header_sample.tsv"
    # )
    # fname_p_sum_template_cdsi: str = (
    #     "cbioportal/intermediate_files/data_clinical_patient_template_cdsi.txt"
    # )
    # fname_s_sum_template_cdsi: str = (
    #     "cbioportal/intermediate_files/data_clinical_sample_template_cdsi.txt"
    # )

    ##############################################################################
    # Generated data files for cbioportal (Datahub)
    ##############################################################################
    # fname_timeline_progression_phi = config_cdm.fname_progression_timeline_cbio
    # fname_timeline_cancer_presence_phi = config_cdm.fname_radiology_cancer_presence_timeline
    # fname_timeline_pathology_mmr_phi = config_cdm.fname_path_mmr_cbio_timeline
    # fname_timeline_ecog_phi = config_cdm.fname_ecog_timeline_cbio

    # summary_p = "data_clinical_patient.txt"
    # summary_s = "data_clinical_sample.txt"
    # timeline_surg = "data_timeline_surgery.txt"
    # timeline_rt = "data_timeline_radiation.txt"
    # timeline_meds = "data_timeline_treatment.txt"
    # timeline_disease_status = "data_timeline_disease_status.txt"
    # timeline_dx_primary = "data_timeline_diagnosis.txt"
    # timeline_spec = "data_timeline_specimen.txt"
    # timeline_spec_surg = "data_timeline_specimen_surgery.txt"
    # timeline_gleason = "data_timeline_gleason.txt"
    # timeline_pdl1 = "data_timeline_pdl1.txt"
    # timeline_pathology_mmr = 'data_timeline_mmr.txt'
    # timeline_prior_meds = "data_timeline_prior_meds.txt"
    # timeline_tumor_sites = "data_timeline_tumor_sites.txt"
    # fname_timeline_fu: str = config_cdm.fname_timeline_fu
    # timeline_follow_up: str = "data_timeline_timeline_follow_up.txt"
    # timeline_progression: str = 'data_timeline_progression.txt'
    # timeline_cancer_presence: str = 'data_timeline_cancer_presence.txt'
    # timeline_ecog_kps: str = 'data_timeline_ecog_kps.txt'
    #
    # fname_summary_patient: str = os.path.join(path_datahub, summary_p)
    # fname_summary_sample: str = os.path.join(path_datahub, summary_s)
    # fname_save_surg_timeline: str = os.path.join(path_datahub, timeline_surg)
    # fname_save_rt_timeline: str = os.path.join(path_datahub, timeline_rt)
    # fname_save_meds_timeline: str = os.path.join(path_datahub, timeline_meds)
    # fname_save_dx_prim_timeline: str = os.path.join(path_datahub, timeline_dx_primary)
    # fname_save_spec_timeline: str = os.path.join(path_datahub, timeline_spec)
    # fname_save_spec_surg_timeline: str = os.path.join(path_datahub, timeline_spec_surg)
    # fname_save_timeline_gleason: str = os.path.join(path_datahub, timeline_gleason)
    # fname_save_timeline_pdl1: str = os.path.join(path_datahub, timeline_pdl1)
    # fname_save_timeline_prior_meds: str = os.path.join(path_datahub, timeline_prior_meds)
    # fname_save_timeline_tumor_sites: str = os.path.join(path_datahub, timeline_tumor_sites)
    # fname_save_timeline_follow_up: str = os.path.join(path_datahub, timeline_follow_up)
    # fname_save_timeline_progression: str = os.path.join(path_datahub, timeline_progression)
    # fname_save_timeline_pathology_mmr: str = os.path.join(path_datahub, timeline_pathology_mmr)
    # fname_save_timeline_cancer_presence: str = os.path.join(path_datahub, timeline_cancer_presence)
    # fname_save_timeline_ecog:  str = os.path.join(path_datahub, timeline_ecog_kps)
    #
    # ##############################################################################
    # # Generated data files for cbioportal (MinIO)
    # ##############################################################################
    # path_minio_cbio = "cbioportal"
    # fname_summary_patient_minio: str = os.path.join(path_minio_cbio, summary_p)
    # fname_summary_sample_minio: str = os.path.join(path_minio_cbio, summary_s)
    # fname_save_surg_timeline_minio: str = os.path.join(path_minio_cbio, timeline_surg)
    # fname_save_rt_timeline_minio: str = os.path.join(path_minio_cbio, timeline_rt)
    # fname_save_meds_timeline_minio: str = os.path.join(path_minio_cbio, timeline_meds)
    # fname_save_dx_prim_timeline_minio: str = os.path.join(path_minio_cbio, timeline_dx_primary)
    # fname_save_spec_timeline_minio: str = os.path.join(path_minio_cbio, timeline_spec)
    # fname_save_spec_surg_timeline_minio: str = os.path.join(path_minio_cbio, timeline_spec_surg)
    # fname_save_timeline_gleason_minio: str = os.path.join(path_minio_cbio, timeline_gleason)
    # fname_save_timeline_pdl1_minio: str = os.path.join(path_minio_cbio, timeline_pdl1)
    # fname_save_timeline_prior_meds_minio: str = os.path.join(path_minio_cbio, timeline_prior_meds)
    # fname_save_timeline_tumor_sites_minio: str = os.path.join(path_minio_cbio, timeline_tumor_sites)
    # fname_save_timeline_follow_up_minio: str = os.path.join(path_minio_cbio, timeline_follow_up)
    #
    # fname_save_timeline_progression_minio: str = os.path.join(path_minio_cbio, timeline_progression)
    # fname_save_timeline_pathology_mmr_minio: str = os.path.join(path_minio_cbio, timeline_pathology_mmr)
    # fname_save_timeline_cancer_presence_minio: str = os.path.join(path_minio_cbio, timeline_cancer_presence)
    # fname_save_timeline_ecog_minio: str = os.path.join(path_minio_cbio, timeline_ecog_kps)
    #
