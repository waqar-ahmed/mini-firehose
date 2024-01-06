import os
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from sinks.local.handlers.local_csv_handler import LocalCSVHandler
import pytest


@pytest.fixture()
def local_csv_handler(tmp_path):
    return LocalCSVHandler(tmp_path)


@pytest.fixture
def sample_data():
    data = [
        {'Salesperson': 'Alice', 'Region': 'East', 'SalesAmount': 310},
        {'Salesperson': 'Charlie', 'Region': 'East', 'SalesAmount': 320},
        {'Salesperson': 'Alice', 'Region': 'North', 'SalesAmount': 230},
        {'Salesperson': 'Bob', 'Region': 'North', 'SalesAmount': 180},
        {'Salesperson': 'Bob', 'Region': 'South', 'SalesAmount': 150},
        {'Salesperson': 'Charlie', 'Region': 'South', 'SalesAmount': 290},
        {'Salesperson': 'Alice', 'Region': 'West', 'SalesAmount': 210},
        {'Salesperson': 'Bob', 'Region': 'West', 'SalesAmount': 200}
    ]
    return pd.DataFrame(data)

@patch("pandas.DataFrame.to_csv")
def test_to_csv_invocation(mock_to_csv, local_csv_handler, sample_data):
    filename = "test.csv"
    local_csv_handler.write(sample_data, filename)
    assert mock_to_csv.called


@patch("pandas.DataFrame.to_csv")
def test_write_csv_file_with_name(mock_to_csv, local_csv_handler, sample_data):
    filename = "test.csv"
    local_csv_handler.write(sample_data, filename)
    mock_to_csv.assert_called_with(os.path.join(local_csv_handler._get_directory(), filename), index=False)


@patch('common.handler.datetime')
@patch("pandas.DataFrame.to_csv")
def test_generate_filename_based_on_datetime(mock_to_csv, mock_datetime, local_csv_handler, sample_data):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    local_csv_handler.write(sample_data)
    mock_to_csv.assert_called_with(os.path.join(local_csv_handler._get_directory(), '20230101120000.csv'), index=False)


@patch('common.handler.Handler._get_filename_based_on', return_value='epoch')
@patch('time.time', return_value=1609459200)
@patch("pandas.DataFrame.to_csv")
def test_generate_filename_based_on_epoch(mock_to_csv, _1, _2, local_csv_handler, sample_data):
    local_csv_handler.write(sample_data)
    assert local_csv_handler._get_filename_based_on() == 'epoch'
    mock_to_csv.assert_called_with(os.path.join(local_csv_handler._get_directory(), '1609459200.csv'), index=False)


def test_actual_csv_file(local_csv_handler, sample_data):
    filename = "test.csv"
    local_csv_handler.write(sample_data, filename)
    assert os.path.exists(os.path.join(local_csv_handler._get_directory(), filename)), f"File does not exist"


@patch('os.makedirs')
@patch("pandas.DataFrame.to_csv")
def test_partitions(mock_to_csv, mock_makedirs, tmp_path, sample_data):
    partition_cols = ['Region']
    local_csv_handler_with_partition_cols = LocalCSVHandler(tmp_path, partition_cols=partition_cols)
    local_csv_handler_with_partition_cols.write(sample_data)
    all_combinations = set(tuple(entry[col] for col in partition_cols) for i, entry in sample_data.iterrows())
    created_directories = {call_args[0][0] for call_args in mock_makedirs.call_args_list}
    expected_directories = [
        os.path.join(local_csv_handler_with_partition_cols._get_directory(), *["{}={}".format(col, val) for col, val in zip(partition_cols, combination)])
        for combination in all_combinations
    ]
    assert sorted(created_directories) == sorted(expected_directories)


@patch('os.makedirs')
@patch("pandas.DataFrame.to_csv")
def test_multi_level_partitions(mock_to_csv, mock_makedirs, tmp_path, sample_data):
    partition_cols = ['Region', 'Salesperson']
    local_csv_handler_with_partition_cols = LocalCSVHandler(tmp_path, partition_cols=partition_cols)
    local_csv_handler_with_partition_cols.write(sample_data)
    all_combinations = set(tuple(entry[col] for col in partition_cols) for i, entry in sample_data.iterrows())
    expected_directories = [
        os.path.join(local_csv_handler_with_partition_cols._get_directory(), *["{}={}".format(col, val) for col, val in zip(partition_cols, combination)])
        for combination in all_combinations
    ]
    # Extract the actual directory names from the mock_makedirs call arguments
    created_directories = {call_args[0][0] for call_args in mock_makedirs.call_args_list}
    assert sorted(created_directories) == sorted(expected_directories)


@pytest.mark.xfail(raises=KeyError)
def test_partition_key_not_found(tmp_path, sample_data):
    partition_cols = ["Region", "Salesperson"]
    local_csv_handler = LocalCSVHandler(tmp_path, partition_cols=partition_cols)
    local_csv_handler.write(sample_data)


def test_no_data(tmp_path):
    with pytest.raises(AttributeError):
        local_csv_handler = LocalCSVHandler(tmp_path)
        local_csv_handler.write(None)
