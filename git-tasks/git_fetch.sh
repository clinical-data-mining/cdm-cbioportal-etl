#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$GITHUB_REPO_PATH"

# GITHUB_REPO_PATH: github repository path
cd "$GITHUB_REPO_PATH"
current_github_branch=$(git rev-parse --abbrev-ref HEAD)

git fetch
git reset --hard origin/$current_github_branch
git pull origin $current_github_branch
# Download relevant LFS files for current commit
# This doesn't actually check for new commits, which is why we had to run `git pull` separately.
if test -n "$LFS_SUBDIR" ; then
    git lfs pull -I "$LFS_SUBDIR"
fi
git clean -f -d