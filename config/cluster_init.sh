#!/bin/bash

set -x
################
# databripy databricks cluster init script.
# Following files are expected to be present on dbfs:
# - wheels folder with latest wheel file: /dbfs/FileStore/wheels/
################

WHEELS_DIR="/dbfs/FileStore/wheels"

echo "Available wheels:"
ls -l $WHEELS_DIR

N=1
ATTEMPTS=3
until [ "$N" -gt "$ATTEMPTS" ]
do
  echo "installing databripy package: attempt $N of $ATTEMPTS" && \
  /databricks/python/bin/pip install databripy --find-links $WHEELS_DIR && \
  break

  N=$((N+1))
  sleep 30
done

if [ "$N" -gt "$ATTEMPTS" ]
then
  echo "failed to install requirements"
  exit 1
fi
