# Databricks notebook source
# Dataframes created in ingestion notebook:
# sdf_pos_raw
# sdf_customers_raw
# sdf_products_raw

import pyspark.sql.functions as sf
from pyspark.sql.window import Window

# COMMAND ----------

# Filter de pos data, we only need data after 2022-06-01
sdf_pos_filtered = sdf_pos_raw.filter(sf.col('date') >= 20220601)

# COMMAND ----------

# MAGIC %md #Transforming raw PoS data

# COMMAND ----------

# Explode the items columns
sdf_pos_expl = sdf_pos_filtered.select(
    sf.col('customer_id'),
    sf.col('order_number'),
    sf.col('date'),
    sf.explode('items').alias('items')
)

# COMMAND ----------

sdf_transformed = sdf_pos_expl.select(
    sf.col('customer_id'),
    sf.col('order_number'),
    sf.col('date'),
    sf.col('items.amount').alias('amount'),
    sf.col('items.product_id').alias('product_id')
)

sdf_transformed = sdf_transformed.join(sdf_customers_raw, how='left', on='customer_id')
sdf_transformed = sdf_transformed.join(sdf_products_raw, how='left', on='product_id')

sdf_transformed = sdf_transformed.select(
    sf.col('customer_id'),
    sf.col('name'),
    sf.col('order_number'),
    sf.col('date'),
    sf.col('amount'),
    sf.col('product_id'),
    sf.col('product_name'),
    (sf.col('cost')*sf.col('amount')).alias('total_cost')
)

# COMMAND ----------

sdf_transformed.orderBy('customer_id').display()
