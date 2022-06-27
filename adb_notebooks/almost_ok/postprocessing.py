# Databricks notebook source
# MAGIC %md #Post-processing data for reporting

# COMMAND ----------

import pyspark.sql.functions as sf

import numpy as np

# COMMAND ----------

# transformed df created in previous notebook: sdf_transformed

# COMMAND ----------

#sdf_average = sdf_transformed.groupby('customer_id', 'order_number').agg((sf.sum('total_cost')/sf.sum('amount')).alias('average'))
sdf_totals = sdf_transformed.groupby('customer_id', 'order_number').agg(sf.sum('total_cost').alias('order_total_cost'), sf.sum('amount').alias('order_total_items'))
sdf_totals = sdf_totals.withColumn('order_average', sf.col('order_total_cost')/sf.col('order_total_items'))

order_average = np.mean([row['order_average'] for row in sdf_totals.select('order_average').drop_duplicates().collect()])

# COMMAND ----------

sdf_complete = sdf_transformed.join(sdf_totals, how='left', on=['customer_id', 'order_number'])
sdf_complete = sdf_complete.withColumn('client_specialness_score', sf.col('order_average')/order_average )
sdf_complete = sdf_complete.withColumn('special_client', sf.when(sf.col('order_average') > order_average, 1).otherwise(0) )

# COMMAND ----------

sdf_final = sdf_complete.select('customer_id', 'client_specialness_score', 'special_client').drop_duplicates()
sdf_final.display()

# COMMAND ----------

# Todo: upsert to delta, just to use some fancy delta stuff
