import os
from datetime import datetime

import pytest
from sinks.local.local_sink import LocalSink
from unittest.mock import patch


@pytest.fixture()
def local_sink(tmp_path):
    return LocalSink(tmp_path, 'csv')

@pytest.fixture
def sample_data():
    return [
        {'Salesperson': 'Alice', 'Region': 'East', 'SalesAmount': 310},
        {'Salesperson': 'Charlie', 'Region': 'East', 'SalesAmount': 320},
        {'Salesperson': 'Alice', 'Region': 'North', 'SalesAmount': 230},
        {'Salesperson': 'Bob', 'Region': 'North', 'SalesAmount': 180},
        {'Salesperson': 'Bob', 'Region': 'South', 'SalesAmount': 150},
        {'Salesperson': 'Charlie', 'Region': 'South', 'SalesAmount': 290},
        {'Salesperson': 'Alice', 'Region': 'West', 'SalesAmount': 210},
        {'Salesperson': 'Bob', 'Region': 'West', 'SalesAmount': 200}
    ]


@patch("pandas.DataFrame.to_csv")
def test_deliver(mock_to_csv, tmp_path, local_sink, sample_data):
        filename = "test.csv"
        local_sink.deliver(sample_data, filename)
        mock_to_csv.assert_called_with(os.path.join(tmp_path, filename), index=False)


def test_no_data(tmp_path):
    with pytest.raises(ValueError) as ex:
        local_sink = LocalSink(tmp_path, 'unknown_type')
    assert "Unsupported output format: unknown_type" in str(ex.value)