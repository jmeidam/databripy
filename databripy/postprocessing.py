import logging
import numpy as np
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as sf


def calculate_order_totals(sdf_pos: DataFrame) -> DataFrame:
    """Calculate the average price per order

    :param sdf_pos:
        The POS data with product information
    """
    logger = logging.getLogger(__name__)
    logger.debug('Calculating average price per order')

    sdf = sdf_pos.groupby('customer_id', 'order_number').agg(
        sf.sum('total_cost').alias('order_total_cost'),
        sf.sum('amount').alias('order_total_items')
    )
    sdf = sdf.withColumn('order_average', sf.col('order_total_cost') / sf.col('order_total_items'))
    return sdf


def calculate_average_over_all_orders(sdf_pos: DataFrame) -> float:
    """Calculate the average price of all orders

    :param sdf_pos:
        The POS data with product information
    """
    logger = logging.getLogger(__name__)
    logger.debug('Calculating average summed order amount')

    order_average = np.mean(
        [row['order_average'] for row in sdf_pos.select('order_average').drop_duplicates().collect()]
    )
    return order_average


def generate_scored_customer_list(sdf_pos: DataFrame) -> DataFrame:
    """Adds client_specialness_score and special_client values to the pos data.

    :param sdf_pos:
        The POS data with product information
    :returns:
        Dataframe with customer_id, client_specialness_score and special_client

    """

    logger = logging.getLogger(__name__)
    logger.debug('Generating scored customer list')

    sdf_totals = calculate_order_totals(sdf_pos)
    summed_average = calculate_average_over_all_orders(sdf_totals)

    sdf = sdf_pos.join(sdf_totals, how='left', on=['customer_id', 'order_number'])
    sdf = sdf.withColumn('client_specialness_score', sf.col('order_average')/summed_average)
    sdf = sdf.withColumn('special_client', sf.when(sf.col('order_average') > summed_average, 1).otherwise(0))

    sdf = sdf.select('customer_id', 'client_specialness_score', 'special_client').drop_duplicates()

    return sdf


def store_list(sdf_list: DataFrame, path: str):
    """Store the list as Delta in the provided path

    :param sdf_list:
    :param path:
        Full path to the Delta table
    """
    logger = logging.getLogger(__name__)
    # Ensure the path is not prefixed with /dbfs
    _path = path.replace('/dbfs', '')
    logger.info(f'Writing list to Delta table at {_path}')

    sdf_list.write.format('delta').save(_path)
