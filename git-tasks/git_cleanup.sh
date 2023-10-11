GITHUB_REPO_PATH=/mind_data/cdm_repos/msk-mind-datahub3/
PUSH_TO_REPO=no
# If we aren't pushing to the remote repo, we want to
# examine the data locally and git clean manually.
# Exit the script without cleaning the git repo.
if [ "$PUSH_TO_REPO" -ne "yes" ]; then
    exit 0
fi

# GITHUB_REPO_PATH: github repository path
cd "$GITHUB_REPO_PATH"
current_github_branch=$(git rev-parse --abbrev-ref HEAD)

echo "git checkout -- ."
git checkout -- . ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git checkout -- .\""
    exit $return_value
fi
echo "git clean -fd"
git clean -fd ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git clean -fd\""
    exit $return_value
fi
echo "git reset --hard origin/$current_github_branch"
git reset --hard origin/$current_github_branch ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command \"git reset --hard origin/$current_github_branch\""
    exit $return_value
fi
echo "git lfs prune"
git lfs prune ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command \"git lfs prune\""
    exit $return_value
fi
exit $return_value