# script to zip up code for docker container.
# usage: 
# 1. Edit this script to select appropriate items, filenames etc
# 2. Run this script from the zip directory which must be a subdir of the base dir you want in the .zip file:  bash zipup.sh
# 3. zip file will be put into current (zip) directory. Copy it to the docker directory (the directory where the dockerfile is)
# NB: -x specifies files to exclude, @ ends the -x portion then -r specifies recurse


rm apis.zip
cd ..
zip -x *.log *.csv *.pyc @ -r ./zip/apis.zip app initializers index.js package.json package-lock.json README.md

