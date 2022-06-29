#!/bin/bash

# First argument is the databricks-cli profile to use
PROFILE=$1
USAGE="Usage: $0 [databricks-cli profile]"


if [ ! "$1" ]; then
  echo "$USAGE"
else

  if [ -d dist ]; then
    rm -r dist build
  fi

  python setup.py bdist_wheel

  dbfs cp config/cluster_init.sh dbfs:/FileStore/databripy/config/cluster_init.sh --profile "$PROFILE" --overwrite
  dbfs cp dist/*.whl dbfs:/FileStore/wheels/ --profile "$PROFILE" --overwrite

  # mkdirs does not do anything if the directory already exists
  databricks workspace mkdirs /projects/project_x --profile "$PROFILE"
  databricks workspace import -l PYTHON --profile "$PROFILE" --overwrite adb_notebooks/main.py /projects/project_x/main

fi
