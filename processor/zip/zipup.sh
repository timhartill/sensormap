# script to zip up code for docker container.
# usage: 
# 1. Edit this script to select appropriate items, filenames etc
# 2. Run this script from the zip directory which must be a subdir of the base dir you want in the .zip file:  bash zipup.sh
# 3. zip file will be put into current (zip) directory. Copy it to the docker directory (the directory where the dockerfile is)
# NB: -x specifies files to exclude, @ ends the -x portion then -r specifies recurse
# NB 2: This version creates the config dir in the zipfile but excludes the .json config files as this dir is volume mapped to external dir in the docker compose file (as is the logs directory)

rm processor.zip
cd ..
zip -x *.log *.csv *.pyc *.json @ -r ./zip/processor.zip config usecasecode logs requirements.txt README.md

