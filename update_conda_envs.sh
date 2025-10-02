#!/bin/bash

# Configuration: Define your environments and their corresponding repo paths
declare -A ENV_REPO_MAP=(
    # Dev environments
    ["conda-env-cdm-fongc2"]="/gpfs/mindphidata/fongc2/github/cdm-cbioportal-etl"
    ["cdm-treatments-dev"]="/gpfs/mindphidata/fongc2/github/cdm-treatments"
    
    # Prod environments
)

# Function to update a single environment
update_env() {
    local env_name=$1
    local repo_path=$2
    
    echo "=========================================="
    echo "Updating environment: $env_name"
    echo "Repository path: $repo_path"
    echo "=========================================="
    
    # Check if repo path exists
    if [ ! -d "$repo_path" ]; then
        echo "ERROR: Repository path $repo_path does not exist!"
        return 1
    fi
    
    # Activate conda environment
    echo "Activating conda environment: $env_name"
    conda activate "$env_name"
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to activate environment $env_name"
        return 1
    fi
    
    # Change to repo directory
    echo "Changing to directory: $repo_path"
    cd "$repo_path" || {
        echo "ERROR: Failed to change to directory $repo_path"
        return 1
    }
    
    # Pull latest changes (optional - uncomment if you want to pull)
    # echo "Pulling latest changes..."
    # git pull
    
    # Install package
    echo "Installing package with pip install ."
    pip install .
    if [ $? -eq 0 ]; then
        echo "SUCCESS: $env_name updated successfully"
    else
        echo "ERROR: Failed to install package in $env_name"
        return 1
    fi
    
    echo ""
}

# Main execution
echo "Starting conda environment updates..."
echo ""

# Track results
declare -a successful_updates=()
declare -a failed_updates=()

# Iterate through all environments
for env_name in "${!ENV_REPO_MAP[@]}"; do
    repo_path="${ENV_REPO_MAP[$env_name]}"
    
    if update_env "$env_name" "$repo_path"; then
        successful_updates+=("$env_name")
    else
        failed_updates+=("$env_name")
    fi
done

# Summary
echo "=========================================="
echo "UPDATE SUMMARY"
echo "=========================================="
echo "Successful updates (${#successful_updates[@]}):"
for env in "${successful_updates[@]}"; do
    echo "  ✓ $env"
done

echo ""
echo "Failed updates (${#failed_updates[@]}):"
for env in "${failed_updates[@]}"; do
    echo "  ✗ $env"
done

echo ""
echo "Update process completed!"