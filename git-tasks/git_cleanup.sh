#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$GITHUB_REPO_PATH"

# GITHUB_REPO_PATH: github repository path
cd "$GITHUB_REPO_PATH"
current_github_branch=$(git rev-parse --abbrev-ref HEAD)

git checkout -- .
git clean -fd
git reset --hard origin/$current_github_branch
git lfs prune # Delete old, unreferenced LFS files from local storage