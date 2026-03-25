# cdm-cbioportal-etl Documentation

Welcome to the cdm-cbioportal-etl documentation! This ETL pipeline transforms clinical data from Databricks into cBioPortal-formatted files using a YAML-based, modular architecture.

## Getting Started

- [Installation Guide](installation.md) - Set up your environment and configure Databricks
- [Architecture Overview](architecture.md) - Understanding the system design
- [Data File Formatting Requirements](data_file_formatting.md) - Input data requirements

## User Guides

### Adding Data
- [Adding New Summary Data](guides/adding_new_summary_data.md) - Add patient or sample summary columns
- [Adding New Timeline Data](guides/adding_new_timeline_data.md) - Add temporal event data

### Running Pipelines
- [Running the Full ETL Pipeline](guides/running_full_etl.md) - Complete end-to-end workflow
- [Troubleshooting Guide](guides/troubleshooting.md) - Common issues and solutions

## Pipeline Documentation

- [Modular Summary Pipeline](pipelines/summary_modular_pipeline.md) - 4-script modular approach for summary files
- [Timeline Deidentification](pipelines/timeline_deidentification.md) - Batch processing for timeline files
- [Monitoring & Validation](pipelines/monitoring.md) - Data quality checks and completeness monitoring

## Configuration Reference

- [Summary YAML Format](configuration/summary_yaml_format.md) - Complete specification for summary configs
- [Timeline YAML Format](configuration/timeline_yaml_format.md) - Complete specification for timeline configs
- [Cohort Configuration Files](configuration/cohort_configs.md) - ETL config files for different cohorts

## Reference Documentation

- [Bash Scripts Reference](reference/bash_scripts.md) - All bash wrapper scripts and their parameters
- [Utility Scripts Reference](reference/utility_scripts.md) - Python utility scripts
- [API Reference](reference/api_reference.md) - Core Python classes and functions

## Legacy Documentation

- [Migration Guide from Old System](archive/MIGRATION_FROM_OLD_SYSTEM.md) - For users of the codebook-based approach
- [Archived Documentation](archive/) - Old documentation (deprecated)

## Quick Links

### Current Configurations
- 14 summary YAML configs in `config/summaries/`
- 21 timeline YAML configs in `config/timelines/`
- 4 cohort configs: mskimpact, mskaccess, mskarcher, mskimpact_heme

### Common Tasks
- [Add a new biomarker summary](guides/adding_new_summary_data.md#example-biomarker)
- [Add a new medication timeline](guides/adding_new_timeline_data.md#example-medications)
- [Run summary pipeline for one cohort](pipelines/summary_modular_pipeline.md#complete-pipeline-example)
- [Run timeline deidentification](pipelines/timeline_deidentification.md#usage)

## Support

If you can't find what you're looking for:
1. Check the [Troubleshooting Guide](guides/troubleshooting.md)
2. Search this documentation
3. File an issue in the repository
