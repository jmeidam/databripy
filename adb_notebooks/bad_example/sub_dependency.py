# Databricks notebook source
from random import randint

# COMMAND ----------

def bogus_function(a, b):
    x = a+b
    y = x+5
    z = y-10
    return x*y*z

# COMMAND ----------

# To demonstrate stack-trace from nested notebooks
def get_random_element(list_thing, idx_min, idx_max):
    idx = randint(idx_min, idx_max)
    element = list_thing[idx]
    return element
