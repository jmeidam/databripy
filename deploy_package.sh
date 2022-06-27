#!/bin/bash

PROFILE=ilionx

if [ -d dist ]; then
  rm -r dist build
fi

python setup.py bdist_wheel

dbfs cp config/cluster_init.sh dbfs:/FileStore/databripy/config/cluster_init.sh --profile $PROFILE --overwrite
dbfs cp dist/*.whl dbfs:/FileStore/wheels/ --profile $PROFILE --overwrite

databricks workspace mkdirs /projects/project_x --profile $PROFILE
databricks workspace import -l PYTHON --profile $PROFILE --overwrite adb_notebooks/main.py /projects/project_x/main.py
