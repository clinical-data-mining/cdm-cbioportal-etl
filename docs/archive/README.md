# Archived Documentation

This directory contains documentation for the **legacy codebook-based system** that was replaced in 2025.

## ⚠️ Important Notice

**This documentation is outdated and maintained only for historical reference.**

If you're looking for current documentation, see:
- [Main Documentation Index](../README.md)
- [Installation Guide](../installation.md)
- [Architecture Overview](../architecture.md)
- [User Guides](../guides/)

## Why Was This Archived?

The cdm-cbioportal-etl pipeline underwent a major refactoring in 2025:

**Legacy System (archived):**
- Configuration via Google Sheets codebook
- MinIO object storage
- Monolithic processing scripts
- Hardcoded timeline configurations

**Current System (active):**
- Configuration via YAML files (version controlled)
- Databricks volumes
- Modular, debuggable pipeline
- YAML-based timeline configurations

See [Migration Guide](MIGRATION_FROM_OLD_SYSTEM.md) for details.

## Archived Files

### Summary Processing
- **`summary_data_formatting.md`** - Documents legacy `RedcapToCbioportalFormat` and `cbioportalSummaryFileCombiner` classes
  - Replaced by: [Modular Summary Pipeline](../pipelines/summary_modular_pipeline.md)

- **`summary_template_generation.md`** - Documents legacy MinIO-based template generation
  - Replaced by: [Running Full ETL](../guides/running_full_etl.md#1-generate-template-files)

### Timeline Processing
- **`timeline_files.md`** - Documents legacy `cbioportal_deid_timeline_files` function with MinIO
  - Replaced by: [Timeline Deidentification](../pipelines/timeline_deidentification.md)

### Configuration
- **`modifying_the_codebook.md`** - Documents Google Sheets codebook workflow
  - Replaced by: [Adding New Summary Data](../guides/adding_new_summary_data.md)

### Migration Documentation
- **`DAG_MIGRATION_GUIDE.md`** - Internal guide documenting the refactoring process (originally in `/sandbox`)
  - Useful for understanding what changed and why

- **`MIGRATION_FROM_OLD_SYSTEM.md`** - User-facing migration guide
  - Use this if you need to migrate from the old system

## When to Reference Archived Documentation

**You might need these docs if:**
1. You're working with very old data that was processed with the legacy system
2. You need to understand historical workflows
3. You're debugging legacy code in the `legacy/` folders
4. You're doing forensic analysis of old outputs

**You should NOT use these docs if:**
1. You're setting up a new pipeline
2. You're adding new data to the system
3. You're troubleshooting current workflows
4. You're onboarding to the project

## Legacy Code Locations

The code described in these docs still exists but is deprecated:

```
pipeline/
├── lib/
│   └── summary/
│       └── legacy/                   # Legacy summary processing classes
│           ├── cbioportal_summary_merger.py
│           ├── cbioportal_template_generator.py
│           ├── cbioportal_summary_file_combiner.py
│           └── create_summary_from_redcap_reports.py
└── bash/
    ├── bash_summary_creator.sh        # Old codebook-based wrapper
    └── bash_yaml_summary_creator.sh   # Old monolithic YAML wrapper
```

**Do not use these files for new development.**

## Archival Date

These files were archived on **2026-03-25** as part of a comprehensive documentation refresh.

## Questions?

If you have questions about:
- **Legacy system:** Review these archived docs or ask someone who worked with the old system
- **Current system:** See [current documentation](../README.md)
- **Migration:** See [Migration Guide](MIGRATION_FROM_OLD_SYSTEM.md)

## Historical Context

### Timeline of Changes

- **Pre-2024:** Codebook-based system fully operational
- **2024 Q3-Q4:** Planning for refactoring to YAML-based approach
- **2025 Q1:** Implementation of YAML configs and Databricks integration
- **2025 Q2:** Deployment of modular pipeline
- **2025 Q3-Q4:** Transition period, both systems operational
- **2026 Q1:** Legacy code moved to `legacy/` folders
- **2026-03-25:** Documentation archived, full transition complete

### Key Contributors

The refactoring was driven by:
- Need for version-controlled configuration
- Consolidation on Databricks platform
- Desire for more debuggable, modular pipeline
- Feedback from users about difficulty adding new data sources

---

**For all current documentation, return to [Main Documentation](../README.md)**
