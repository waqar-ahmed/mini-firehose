import os
from datetime import datetime

import pytest
from sinks.local.handlers.local_handler import LocalHandler
from unittest.mock import patch


@pytest.fixture()
def local_handler(tmp_path):
    return LocalHandler(tmp_path, 'csv')


def test_base_directory(tmp_path, local_handler):
    assert local_handler._get_directory() == tmp_path


@patch('common.handler.datetime')
def test_get_file_path_with_no_name(mock_datetime, local_handler):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    expected_file_path = os.path.join(local_handler._get_directory(), '20230101120000.csv')
    assert local_handler.get_file_path() == expected_file_path


def test_get_file_path_with_name(local_handler):
    expected_file_path = os.path.join(local_handler._get_directory(), 'test.csv')
    assert local_handler.get_file_path('test.csv') == expected_file_path


@patch('common.handler.datetime')
def test_get_partition_path_single_level(mock_datetime, tmp_path):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    filename = "20230101120000.csv"
    partition_cols = ["Region"]
    local_handler = LocalHandler(tmp_path, 'csv', partition_cols=partition_cols)
    expected_path = os.path.join(local_handler._get_directory(), "Region=North", filename)
    assert expected_path == local_handler.get_partition_path("North")


@patch('common.handler.datetime')
def test_get_partition_path_multi_level(mock_datetime, tmp_path):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    filename = "20230101120000.csv"
    partition_cols = ["Region", "SalePerson"]
    local_handler = LocalHandler(tmp_path, 'csv', partition_cols=partition_cols)
    expected_path = os.path.join(local_handler._get_directory(), "Region=North", "SalePerson=John", filename)
    assert expected_path == local_handler.get_partition_path(("North", "John"))

    partition_cols = ["A", "B", "C"]
    local_handler = LocalHandler(tmp_path, 'csv', partition_cols=partition_cols)
    expected_path = os.path.join(local_handler._get_directory(), "A=a", "B=b", "C=c", filename)
    assert expected_path == local_handler.get_partition_path(("a", "b", "c"))