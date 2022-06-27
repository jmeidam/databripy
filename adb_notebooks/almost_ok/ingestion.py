# Databricks notebook source
# MAGIC %md #Ingesting data

# COMMAND ----------

sdf_pos_raw = spark.read.format('json').schema(spark_schema_pos).load(path_pos_raw.replace('/dbfs', ''))
sdf_customers_raw = spark.read.format('csv').option('header', 'true').load(path_cust_raw.replace('/dbfs', ''))
sdf_products_raw = spark.read.format('csv').option('header', 'true').load(path_prod_raw.replace('/dbfs', ''))
