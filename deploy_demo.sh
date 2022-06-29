#!/bin/bash

# First argument is the databricks username
# Second argument is the databricks-cli profile to use
USAGE="Usage: $0 [databricks-cli profile]"

if [ ! "$1" ]; then
  echo "$USAGE"
else
  databricks workspace mkdirs /projects/datadays --profile "$PROFILE"
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/almost_ok "/projects/datadays/almost_ok"
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/bad_example "/projects/datadays/bad_example"
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/tools "/projects/datadays/tools"
fi

