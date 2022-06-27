import os
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType


def get_config(path_raw_root: str, path_processed_root: str) -> dict:
    """Creates dictionary with paths and settings

    :param path_raw_root:
        Root path to where the raw data is located
    :param path_processed_root:
        Root path to where the processed data must be written
    """
    logger = logging.getLogger()
    logger.info(
        f'Setting up configuration with raw root path {path_raw_root} and processed root path {path_processed_root}')

    # Paths without extensions are Delta, or nested data
    data_sets = {
        'pos_raw': {
            'path': os.path.join(path_raw_root, 'pos'),
            'format': 'json'
        },
        'customers_raw': {
            'path': os.path.join(path_raw_root, 'customers.csv'),
            'format': 'csv'
        },
        'products_raw': {
            'path': os.path.join(path_raw_root, 'products.csv'),
            'format': 'csv'
        },
        'scored_customer_list': {
            'path': os.path.join(path_processed_root, 'list'),
            'format': 'delta'
        }
    }

    settings = {
        'date_from': 20220601
    }

    conf = {
        'data_sets': data_sets,
        'settings': settings
    }

    return conf


def get_pos_schema():
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
    return spark_schema_pos
