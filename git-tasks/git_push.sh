#!/bin/bash
set -eE -v
trap 'echo "Last command exited with status code of $?, exiting..."' ERR

test -n "$GITHUB_REPO_PATH"
test -n "$COMMIT_MSG"

cd "$GITHUB_REPO_PATH"
git add *
# `git commit` will exit 1 if there is nothing to commit, ignore the error in that case
git commit -m "$COMMIT_MSG" || true
git push origin