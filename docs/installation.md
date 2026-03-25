# Installation Guide

This guide walks you through setting up the cdm-cbioportal-etl pipeline.

## Prerequisites

- Conda or Miniconda installed
- Access to Databricks workspace
- Git

## Step 1: Clone the Repository

```bash
git clone https://github.com/clinical-data-mining/cdm-cbioportal-etl.git
cd cdm-cbioportal-etl
```

## Step 2: Create Conda Environment

### Option A: Using environment.yml

```bash
conda env create -f environment.yml
conda activate cdm-cbioportal-etl
```

This creates an environment called `cdm-cbioportal-etl` with:
- Python 3.11
- NumPy 1.23.5
- PyZMQ 25.1.2
- msk_cdm package (from GitHub)

### Option B: Install into existing environment

```bash
# Activate your existing environment
conda activate your-env

# Install the package
pip install .
```

## Step 3: Configure Databricks Connection

Create a Databricks environment file containing your connection credentials.

### File format

Create a text file (e.g., `databricks_env.txt`) with:

```bash
DATABRICKS_HOST=https://your-databricks-instance.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

### Getting your Databricks credentials

1. **Host:** Your Databricks workspace URL
2. **Token:** Generate a personal access token:
   - Go to User Settings → Access Tokens
   - Click "Generate New Token"
   - Copy the token (save it securely - you won't see it again!)

### Secure storage

**Important:** Never commit your Databricks credentials to git!

Add to `.gitignore`:
```
databricks_env*.txt
*_env.txt
```

Store your credentials file in a secure location outside the repository:
```bash
# Example: store in home directory
~/databricks_env_prod.txt
~/databricks_env_dev.txt
```

## Step 4: Verify Installation

Test that everything is installed correctly:

```bash
# Activate environment
conda activate cdm-cbioportal-etl

# Test Python imports
python -c "from msk_cdm.databricks import DatabricksAPI; print('✓ Installation successful!')"
```

## Step 5: Test Databricks Connection

```python
from msk_cdm.databricks import DatabricksAPI

# Initialize with your env file
db = DatabricksAPI(fname_databricks_env='/path/to/databricks_env.txt')

# Test connection by listing catalogs
catalogs = db.spark.sql("SHOW CATALOGS").toPandas()
print(f"✓ Connected! Found {len(catalogs)} catalogs")
```

## Directory Structure

After installation, your repository should look like:

```
cdm-cbioportal-etl/
├── config/
│   ├── summaries/          # 14 summary YAML configs
│   ├── timelines/          # 21 timeline YAML configs
│   ├── cbioportal_headers/ # Header templates
│   └── etl_config_*.yml    # Cohort configs
├── docs/                   # Documentation
├── pipeline/
│   ├── summary/           # Summary pipeline scripts
│   ├── timeline/          # Timeline pipeline scripts
│   ├── bash/              # Bash wrapper scripts
│   ├── lib/               # Core library code
│   └── utils/             # Utility scripts
└── environment.yml        # Conda environment spec
```

## Troubleshooting

### Issue: Conda environment creation fails

**Solution:** Update conda first:
```bash
conda update -n base conda
conda env create -f environment.yml
```

### Issue: msk_cdm installation fails

**Solution:** The package is installed from GitHub. Ensure you have:
- Git installed
- Network access to GitHub
- SSH keys configured (if using SSH URLs)

### Issue: Databricks connection fails

**Solutions:**
1. Verify your host URL is correct (including `https://`)
2. Check your token is valid (tokens can expire)
3. Ensure you have network access to Databricks
4. Verify your environment file format is correct (no quotes around values)

### Issue: Permission denied when accessing Databricks tables

**Solution:** Your Databricks token needs appropriate permissions:
- Read access to source catalogs/schemas
- Write access to destination volumes
- Execute permissions for SQL queries

Contact your Databricks administrator to grant necessary permissions.

## Next Steps

- Read the [Architecture Overview](architecture.md) to understand the system
- Follow [Adding New Summary Data](guides/adding_new_summary_data.md) to add your first summary
- See [Running the Full ETL Pipeline](guides/running_full_etl.md) for complete workflow

## Environment Variables Reference

The Databricks environment file supports these variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_HOST` | Yes | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | Yes | Personal access token |

Additional environment setup may be required for specific pipelines. See individual pipeline documentation for details.
