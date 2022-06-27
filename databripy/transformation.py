import logging
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as sf


def expand_pos_json_field(sdf_pos: DataFrame) -> DataFrame:
    """Expands the items field into one row per purchase
    Adds two columns: amount and product_id

    :param sdf_pos:
        The POS dataframe
    """

    # Explode the items field
    sdf = sdf_pos.select(
        sf.col('customer_id'),
        sf.col('order_number'),
        sf.col('date'),
        sf.explode('items').alias('items')
    )

    # Select and rename required columns
    sdf = sdf.select(
        sf.col('customer_id'),
        sf.col('order_number'),
        sf.col('date'),
        sf.col('items.amount').alias('amount'),
        sf.col('items.product_id').alias('product_id')
    )

    return sdf


def join_dimensional_data(sdf_pos: DataFrame, sdf_customers: DataFrame, sdf_products: DataFrame) -> DataFrame:
    """Left-joins customer and product data onto POS data.
    sdf_pos must contain the product_id and customer_id fields

    :param sdf_pos:
        The POS dataframe
    :param sdf_customers:
        Customer dimensional data with customer_id key
    :param sdf_products:
        Products dimensional data with product_id key
    """

    if 'customer_id' not in sdf_pos.columns or 'product_id' not in sdf_pos.columns:
        raise ValueError('One of the ID columns is missing from sdf_pos')

    logger = logging.getLogger(__name__)
    logger.info('Joining dimensional product and customer data onto POS data')

    sdf = sdf_pos.join(sdf_customers, how='left', on='customer_id')
    sdf = sdf.join(sdf_products, how='left', on='product_id')

    sdf = sdf.select(
        sf.col('customer_id'),
        sf.col('name'),
        sf.col('order_number'),
        sf.col('date'),
        sf.col('amount'),
        sf.col('product_id'),
        sf.col('product_name')
    )

    return sdf


def calculate_total_cost_column(sdf_pos_enriched: DataFrame) -> DataFrame:
    """Calculate the total cost for each line item from the cost and amount columns

    :param sdf_pos_enriched:
        The POS dataframe enriched with product data
    """
    logger = logging.getLogger(__name__)
    logger.debug('Calculating total_cost column')

    if 'cost' not in sdf_pos_enriched.columns or 'amount' not in sdf_pos_enriched.columns:
        raise ValueError('One of the cost or amount columns is missing from sdf_pos')

    total_cost = sf.col('cost')*sf.col('amount')

    return sdf_pos_enriched.withColumn('total_cost', total_cost)



