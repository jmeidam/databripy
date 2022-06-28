from databripy import ingestion


def test_add_options_to_spark_reader(mocker):
    reader = mocker.Mock()
    reader.option = mocker.Mock(return_value=reader)
    reader.schema = mocker.Mock(return_value=reader)

    options = {
        'header': 'true',
        'schema': 'schema_object'
    }

    ingestion.add_options_to_spark_reader(
        reader, 'json', options)

    reader.option.assert_called_once_with('header', 'true')
    reader.schema.assert_called_once_with('schema_object')


def test_read_data(mocker):

    spark_session = mocker.Mock()
    reader = mocker.Mock()
    spark_session.read = reader

    reader.format = mocker.Mock(return_value=reader)
    reader.load = mocker.Mock()
    foptions = mocker.patch('databripy.ingestion.add_options_to_spark_reader', return_value=reader)

    ingestion.read_data(spark_session, 'json', 'path/to/data', header='true')

    reader.format.assert_called_once_with('json')
    foptions.assert_called_once_with(reader, 'json', {'header': 'true'})
    reader.load.assert_called_once_with('path/to/data')


def test_limit_pos_data(spark_session):
    sdf = spark_session.createDataFrame([
        [20220601],
        [20220602],
        [20220603],
        [20220604],
        [20220605],
    ], ['date'])

    sdf_returned = ingestion.limit_pos_dates(sdf, 20220604)

    assert sdf_returned.count() == 2
