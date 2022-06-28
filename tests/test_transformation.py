from databripy import transformation


def test_expand_json_field(spark_session):
    columns = ['customer_id', 'order_number', 'date', 'items']
    items1 = [
        {'product_id': 'a1', 'amount': 5},
        {'product_id': 'a2', 'amount': 4},
        {'product_id': 'a3', 'amount': 3}
    ]
    items2 = [
        {'product_id': 'b1', 'amount': 2},
        {'product_id': 'b2', 'amount': 3}
    ]
    sdf_struct = spark_session.createDataFrame(
        [
            [1, 11, 20220601, items1],
            [2, 22, 20220602, items2]
        ], columns
    )

    sdf_returned = transformation.expand_pos_json_field(sdf_struct)
    columns_expected = ['customer_id', 'order_number', 'date', 'amount', 'product_id']
    n_rows_expected = 5

    assert sorted(sdf_returned.columns) == sorted(columns_expected)
    assert sdf_returned.count() == n_rows_expected


# No more unit tests for this demo
