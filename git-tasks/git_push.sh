GITHUB_REPO_PATH=/mind_data/cdm_repos/msk-mind-datahub6/
PROGRAM_NAME=CDM_GIT_PUSH_TEST
PUSH_TO_REPO=yes
# GITHUB_REPO_PATH: github repository path
cd "$GITHUB_REPO_PATH"

git status
echo "git add *"
git add * ; return_value=$?
if [ $return_value -gt 0 ] ; then
    echo "Return value of $return_value for command: \"git add *\""
    exit $return_value
fi
git commit -m "Latest GDC Dataset: $PROGRAM_NAME"
if [ "$PUSH_TO_REPO" = "yes" ]; then
    echo "git push origin"
    git push origin ; return_value=$?
    if [ $return_value -gt 0 ] ; then
        echo "Return value of $return_value for command: \"git push origin\""
        exit $return_value
    fi
fi
exit $return_value
