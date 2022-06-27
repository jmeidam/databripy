# Databricks notebook source
import os
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DateType

# COMMAND ----------

raw_data_root_path = '/dbfs/data/raw'
path_pos = os.path.join(raw_data_root_path, 'pos')
path_cust = os.path.join(raw_data_root_path, 'customers')
path_prod = os.path.join(raw_data_root_path, 'products')

# COMMAND ----------

processed_data_root_path = '/dbfs/data/processed'
path_pos = os.path.join(processed_data_root_path, 'pos')
path_cust = os.path.join(processed_data_root_path, 'customers')
path_prod = os.path.join(processed_data_root_path, 'products')

# COMMAND ----------

spark_schema_pos = StructType([
    StructField('date', DateType()),
    StructField('customer_id', StringType()),
    StructField('datetime', StringType()),
    StructField('order_number', StringType()),
    StructField('items', ArrayType(
        StructType([
            StructField('amount', FloatType()),
            StructField('product_id', StringType())
        ])
    ))
])
