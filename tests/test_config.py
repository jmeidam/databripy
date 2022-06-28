from pyspark.sql.types import StructType

from databripy import config


def test_get_config():
    # Only testing that this returns a dict with data_sets and settings
    cfg = config.get_config('path1', 'path2')
    assert isinstance(cfg, dict)
    assert 'data_sets' in cfg.keys()
    assert 'settings' in cfg.keys()


def test_get_pos_schema_return_type():
    schema = config.get_pos_schema()
    assert isinstance(schema, StructType)
