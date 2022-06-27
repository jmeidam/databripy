import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader
import pyspark.sql.functions as sf


def add_options_to_spark_reader(reader: DataFrameReader, data_format: str, options: dict) -> DataFrameReader:
    """Add any set of options to a Spark DataFrameReader object

    :param reader:
    :param data_format:
        For logging purposes
    :param options:
        Dictionary containing options recognised by the DataFrameReader option() or schema() methods
    """
    logger = logging.getLogger(__name__)
    for key, value in options:
        if key == 'schema':
            logger.debug(f'Setting schema for {data_format} reader')
            reader = reader.schema(value)
        else:
            logger.debug(f'Adding option to {data_format} reader: {key}={value}')
            reader.option(key, value)
    return reader


def read_data(
        spark_session: SparkSession, data_format: str, path: str, **options) -> DataFrame:
    """Reads data from given path into a Spark dataframe

    :param spark_session:
    :param data_format:
        Any data format that is understood by Spark's DataFrameReader
    :param path:
    :param options:
        key-word arguments for options to be added to the DataFrameReader (using the option() and schema() methods)
    """

    logger = logging.getLogger(__name__)

    # Ensure the path is not prefixed with /dbfs
    _path = path.replace('/dbfs', '')
    logger.info(f'Reading data from path {_path}')

    reader = spark_session.read.format(data_format)
    reader = add_options_to_spark_reader(reader, data_format, options)

    return reader.load(_path)


def limit_pos_dates(sdf_pos: DataFrame, from_date: int, date_col_name: str = 'date') -> DataFrame:
    """Sets a lower limit to the dates in POS data

    :param sdf_pos:
        POS dataframe
    :param from_date:
        integer minimum date
    :param date_col_name:
        Name of the date column to apply filter to
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Applying filter to POS-data: {date_col_name} >= {from_date}')
    return sdf_pos.filter(sf.col(date_col_name) >= from_date)
