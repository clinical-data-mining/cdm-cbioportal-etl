import os
from dataclasses import dataclass


root_path: str = "/gpfs/mindphidata/cdm_repos/github"
@dataclass
class CDMProcessingVariablesCbioportal:
    root_path_cbio_etl: str = os.path.join(root_path, "cdm-cbioportal-etl")

    ##############################################################################
    # cBioPortal summary and timeline scripts
    ##############################################################################
    path_map: str = "/config/redcap_report_api_map_cdm.csv"
    path_vars: str = "/config/redcap_variables_cdm.csv"
    redcap_rpt_map: str = "redcap_exports/cdm_cbioportal_codebook/Extraction_of_Clinical_Data_for_MSK-IMPACT_patients__MSK-MIND__redcap_report_mapping.tsv"
    path_redcap_dest: str = "redcap_exports/cdm_cbioportal_codebook"
    config_redcap: str = "-t {{ params.TOKEN }} -u {{ params.URL }} -map {{ params.ID }} -vars {{params.VARS}} -dest {{ params.PATH }}  -minio {{ params.MINIO }}"

    ##############################################################################
    # cBioPortal setup files
    ##############################################################################
    path_datahub_testing: str = "/gpfs/mindphidata/fongc2/datahubs/cdm/msk-chord/"

    # For IMPACT transition with cbioportal backend team
    path_datahub: str = "/gpfs/mindphidata/cdm_repos/datahub/cdm-data/"
    fname_cbio_header_template_p: str = (
        "cbioportal/cbioportal_summary_header_patient.tsv"
    )
    fname_cbio_header_template_s: str = (
        "cbioportal/cbioportal_summary_header_sample.tsv"
    )
    fname_p_sum_template_cdsi: str = (
        "cbioportal/intermediate_files/data_clinical_patient_template_cdsi.txt"
    )
    fname_s_sum_template_cdsi: str = (
        "cbioportal/intermediate_files/data_clinical_sample_template_cdsi.txt"
    )

    ##############################################################################
    # Generated data files for cbioportal (Datahub)
    ##############################################################################
    summary_p = "data_clinical_patient.txt"
    summary_s = "data_clinical_sample.txt"
    timeline_surg = "data_timeline_surgery.txt"
    timeline_rt = "data_timeline_radiation.txt"
    timeline_meds = "data_timeline_treatment.txt"
    timeline_disease_status = "data_timeline_disease_status.txt"
    timeline_dx_primary = "data_timeline_diagnosis.txt"
    # timeline_dx_met = 'data_timeline_indication_of_mets.txt'         # 2023/10/18 Turned off
    # timeline_dx_ln = 'data_timeline_ln.txt'         # 2023/10/18 Turned off
    timeline_spec = "data_timeline_specimen.txt"
    timeline_spec_surg = "data_timeline_specimen_surgery.txt"
    # timeline_progression = 'data_timeline_progression.txt'         # 2023/10/18 Turned off
    # timeline_cea = 'data_timeline_cea.txt'         # 2023/10/18 Turned off
    timeline_gleason = "data_timeline_gleason.txt"
    timeline_pdl1 = "data_timeline_pdl1.txt"
    timeline_prior_meds = "data_timeline_prior_meds.txt"
    timeline_tumor_sites = "data_timeline_tumor_sites.txt"

    fname_timeline_fu: str = "demographics/table_timeline_follow_up.tsv"
    timeline_follow_up: str = "data_timeline_timeline_follow_up.txt"

    fname_summary_patient: str = os.path.join(path_datahub, summary_p)
    fname_summary_sample: str = os.path.join(path_datahub, summary_s)
    fname_save_surg_timeline: str = os.path.join(path_datahub, timeline_surg)
    fname_save_rt_timeline: str = os.path.join(path_datahub, timeline_rt)
    fname_save_meds_timeline: str = os.path.join(path_datahub, timeline_meds)
    # fname_save_disease_status_timeline: str = os.path.join(path_datahub, timeline_disease_status)         # 2023/10/18 Turned off
    fname_save_dx_prim_timeline: str = os.path.join(path_datahub, timeline_dx_primary)
    # fname_save_dx_met_timeline: str = os.path.join(path_datahub, timeline_dx_met)         # 2023/10/18 Turned off
    # fname_save_dx_ln_timeline: str = os.path.join(path_datahub, timeline_dx_ln)         # 2023/10/18 Turned off
    fname_save_spec_timeline: str = os.path.join(path_datahub, timeline_spec)
    fname_save_spec_surg_timeline: str = os.path.join(path_datahub, timeline_spec_surg)
    # fname_save_progression: str = os.path.join(path_datahub, timeline_progression)         # 2023/10/18 Turned off
    # fname_save_labs_cea: str = os.path.join(path_datahub, timeline_cea)         # 2023/10/18 Turned off
    fname_save_timeline_gleason: str = os.path.join(path_datahub, timeline_gleason)
    fname_save_timeline_pdl1: str = os.path.join(path_datahub, timeline_pdl1)
    fname_save_timeline_prior_meds: str = os.path.join(
        path_datahub, timeline_prior_meds
    )
    fname_save_timeline_tumor_sites: str = os.path.join(
        path_datahub, timeline_tumor_sites
    )
    fname_save_timeline_follow_up: str = os.path.join(path_datahub, timeline_follow_up)

    ##############################################################################
    # Generated data files for cbioportal (MinIO)
    ##############################################################################
    path_minio_cbio = "cbioportal"
    fname_summary_patient_minio: str = os.path.join(path_minio_cbio, summary_p)
    fname_summary_sample_minio: str = os.path.join(path_minio_cbio, summary_s)
    fname_save_surg_timeline_minio: str = os.path.join(path_minio_cbio, timeline_surg)
    fname_save_rt_timeline_minio: str = os.path.join(path_minio_cbio, timeline_rt)
    fname_save_meds_timeline_minio: str = os.path.join(path_minio_cbio, timeline_meds)
    # fname_save_disease_status_timeline_minio: str = os.path.join(path_minio_cbio, timeline_disease_status)         # 2023/10/18 Turned off
    fname_save_dx_prim_timeline_minio: str = os.path.join(
        path_minio_cbio, timeline_dx_primary
    )
    # fname_save_dx_met_timeline_minio: str = os.path.join(path_minio_cbio, timeline_dx_met)         # 2023/10/18 Turned off
    # fname_save_dx_ln_timeline_minio: str = os.path.join(path_minio_cbio, timeline_dx_ln)         # 2023/10/18 Turned off
    fname_save_spec_timeline_minio: str = os.path.join(path_minio_cbio, timeline_spec)
    fname_save_spec_surg_timeline_minio: str = os.path.join(
        path_minio_cbio, timeline_spec_surg
    )
    # fname_save_progression_minio: str = os.path.join(path_minio_cbio, timeline_progression)         # 2023/10/18 Turned off
    # fname_save_labs_cea_minio: str = os.path.join(path_minio_cbio, timeline_cea)         # 2023/10/18 Turned off
    fname_save_timeline_gleason_minio: str = os.path.join(
        path_minio_cbio, timeline_gleason
    )
    fname_save_timeline_pdl1_minio: str = os.path.join(path_minio_cbio, timeline_pdl1)
    fname_save_timeline_prior_meds_minio: str = os.path.join(
        path_minio_cbio, timeline_prior_meds
    )
    fname_save_timeline_tumor_sites_minio: str = os.path.join(
        path_minio_cbio, timeline_tumor_sites
    )
    fname_save_timeline_follow_up_minio: str = os.path.join(
        path_minio_cbio, timeline_follow_up
    )
