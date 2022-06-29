# Databricks notebook source
import os
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType

# COMMAND ----------

# MAGIC %md # Setting up variables

# COMMAND ----------

raw_data_root_path = '/dbfs/data/raw'
path_pos_raw = os.path.join(raw_data_root_path, 'pos')
path_cust_raw = os.path.join(raw_data_root_path, 'customers.csv')
path_prod_raw = os.path.join(raw_data_root_path, 'products.csv')

# COMMAND ----------

processed_data_root_path = '/dbfs/data/processed'
path_pos_processed = os.path.join(processed_data_root_path, 'pos')
path_cust_processed = os.path.join(processed_data_root_path, 'customers')
path_prod_processed = os.path.join(processed_data_root_path, 'products')

# COMMAND ----------

spark_schema_pos = StructType([
    StructField('date', IntegerType()),
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
