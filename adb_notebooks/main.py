# Databricks notebook source
import logging

from databripy import etl

# COMMAND ----------
# Parse parameters
dbutils.widgets.text("path_raw_root", "", "Root path to raw data")
path_raw_root = dbutils.widgets.get("path_raw_root")
dbutils.widgets.text("path_processed_root", "", "Root path to processed data")
path_processed_root = dbutils.widgets.get("path_processed_root")

# COMMAND ----------

etl.main(spark, path_raw_root, path_processed_root, log_level=logging.DEBUG)

# COMMAND ----------
