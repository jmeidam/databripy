#!/bin/bash

# First argument is the databricks username
# Second argument is the databricks-cli profile to use
USAGE="Usage: $0 [databricks accountname] [databricks-cli profile]"

if [ ! "$1" ] || [ ! "$2" ]; then
  echo "$USAGE"
else
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/almost_ok "/Users/$1/datadays/almost_ok"
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/bad_example "/Users/$1/datadays/bad_example"
  databricks workspace import_dir --profile "$1" --overwrite adb_notebooks/tools "/Users/$1/datadays/tools"
fi

