import logging
from pyspark.sql import SparkSession
from databripy import ingestion, config, transformation, postprocessing


from databripy import __name__ as project_name


def init_logger(name: str = None, level: int = logging.DEBUG) -> logging.Logger:
    """
    Initialize a logger

    :param name:
        If None, will use failure_recovery_plan.__name__
    :param level:
        Logging level used in the StreamHandler
    """

    if not name:
        name = project_name
    logger = logging.getLogger(name)
    # Ensure the lowest logging level can be used by the handlers
    logger.setLevel(level)

    formatstring = '%(asctime)s|%(name)s:%(funcName)s()|%(levelname)s|%(message)s'
    formatter = logging.Formatter(formatstring)

    # initializing a logger means removing any existing handlers and starting from scratch
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(level)
    logger.addHandler(sh)

    return logger


def main(spark_session: SparkSession, path_raw_root: str, path_processed_root: str, log_level: int = logging.INFO):
    """Main project flow/entry-point

    :param spark_session:
    :param path_raw_root:
        Root path to raw data
    :param path_processed_root:
        Root path to processed data
    :param log_level:
        Standard logger log level (logging.INFO by default)
    """

    logger = init_logger(level=log_level)
    logger.info('Starting project')
    logger.info(f'Root path raw data: {path_raw_root}')
    logger.info(f'Root path processed data: {path_processed_root}')

    cfg = config.get_config(path_raw_root, path_processed_root)
    spark_schema_pos = config.get_pos_schema()

    data_sets = cfg['data_sets']
    settings = cfg['settings']

    # Load pos data and limit dates
    sdf_pos_raw = ingestion.read_data(
        spark_session, data_sets['pos_raw']['format'], data_sets['pos_raw']['path'], schema=spark_schema_pos)
    sdf_pos_raw = ingestion.limit_pos_dates(sdf_pos_raw, settings['date_from'])

    # Load dim tables
    sdf_customers_raw = ingestion.read_data(
        spark_session, data_sets['customers_raw']['format'], data_sets['customers_raw']['path'], header='true')
    sdf_products_raw = ingestion.read_data(
        spark_session, data_sets['products_raw']['format'], data_sets['products_raw']['path'], header='true')

    # transform and expand the pos data
    sdf_pos = transformation.expand_pos_json_field(sdf_pos_raw)
    sdf_pos = transformation.join_dimensional_data(sdf_pos, sdf_customers_raw, sdf_products_raw)
    sdf_pos = transformation.calculate_total_cost_column(sdf_pos)

    # Post-process the data and build the target list
    sdf_list = postprocessing.generate_scored_customer_list(sdf_pos)

    # Store the list to Delta
    postprocessing.store_list(sdf_list, data_sets['scored_customer_list']['path'])





