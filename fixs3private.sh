#!/bin/bash

set -eu

ds="$1"

git clone --depth 1 --no-single-branch  https://github.com/OpenNeuroDatasets/$ds.git

cd $ds
git config annex.private true
git annex init
git annex enableremote s3-PRIVATE autoenable=false
git remote rm s3-PRIVATE
git log -p origin/git-annex..git-annex | tee ../$ds.diff

if [ `grep -c '^commit ' "../$ds.diff"` != 1 ]; then
    echo "Got not just 1 commit:"
    cat ../$ds.diff
    exit 1;
fi

echo "to fix just:"
echo git push origin git-annex
