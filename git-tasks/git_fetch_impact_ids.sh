GITHUB_REPO_PATH=/mind_data/cdm_repos/datahubs/msk-impact/ 
cd "$GITHUB_REPO_PATH"
current_github_branch=$(git rev-parse --abbrev-ref HEAD)

echo "git fetch"
git fetch ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git fetch\""
    exit $return_value
fi
echo "git reset --hard origin/$current_github_branch"
git reset --hard origin/$current_github_branch ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git fetch\""
    exit $return_value
fi
echo "git lfs pull"
git lfs pull ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git lfs pull\""
    exit $return_value
fi
echo "git clean -f -d"
git clean -f -d ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git clean -f -d\""
    exit $return_value
fi
exit $return_value
