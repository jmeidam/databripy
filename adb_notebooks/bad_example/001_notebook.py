# Databricks notebook source
# COMMAND ----------

import os
import sys
import datetime

from pyspark.sql.functions import sum, count, col, when

# COMMAND ----------

# MAGIC %run ./dependency

# COMMAND ----------

Data_frame = spark.read.format('csv').option('header', 'true').load(PATH_DATA)

# COMMAND ----------

DATA_frame_HighCost = Data_frame.select(
  col('cost'),
  col('product_name'),
  when(col('cost') >  30.0,      1).otherwise(0).alias('high_price')
)

DATA_frame_HighCost.display()

# COMMAND ----------

DATA_frame_HighCostCount = DATA_frame_HighCost.groupBy(
          col('high_price')
).agg(
  count(col('high_price')),
  sum(col('cost'))
)

DATA_frame_HighCostCount.display()

# COMMAND ----------

DATA_frame_HighCostCount.write.format('delta').save('/dbfs/data/test/high_cost_count.csv')

# COMMAND ----------

get_random_element([1,2,3,4,5], 6, 10)

# COMMAND ----------
for path in ['/dbfs/data/', '/dbfs/data/raw']:
  %ls $path



