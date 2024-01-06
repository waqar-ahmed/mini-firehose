import os
from datetime import datetime
from unittest.mock import patch

import botocore
from botocore.exceptions import ClientError
from moto import mock_s3
from sinks.s3.handlers.s3_csv_handler import S3CSVHandler
import pytest
import boto3

bucket = "testbucket"
prefix = "testprefix"


@pytest.fixture()
def mock_boto():
    with mock_s3():
        res = boto3.resource('s3')
        res.create_bucket(Bucket=bucket, CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-1'
        })
        yield


@pytest.fixture()
def s3_csv_handler():
    return S3CSVHandler(bucket, prefix)


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


def list_all_buckets():
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    for s3_bucket in response['Buckets']:
        print(s3_bucket['Name'])


def list_objects(bucket_name, obj_prefix=None):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name}

    s3_objects = []

    if obj_prefix:
        operation_parameters['Prefix'] = obj_prefix

    for page in paginator.paginate(**operation_parameters):
        for obj in page.get('Contents', []):
            print(obj['Key'])
            s3_objects.append(f"s3://{bucket_name}/{obj['Key']}")

    return s3_objects


def key_exists(s3_bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=s3_bucket, Key=key)
        print(f"Key: '{key}' found!")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Key: '{key}' does not exist!")
            return False
        else:
            print("Something else went wrong")
            raise


def test_write_csv_file_to_s3(mock_boto, s3_csv_handler, sample_data):
    filename = "test.csv"
    s3_csv_handler.write(sample_data, filename)
    assert key_exists(bucket, "/".join([prefix, filename])), f"File does not exist"


@patch('common.handler.datetime')
def test_write_partitioned_data_to_s3(mock_datetime, mock_boto, sample_data):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    filename = '20230101120000.csv'
    partition_cols = ["Region"]
    s3_csv_handler = S3CSVHandler(bucket, prefix, partition_cols=partition_cols)
    s3_csv_handler.write(sample_data)
    all_combinations = set(tuple(entry[col] for col in partition_cols) for entry in sample_data)
    expected_objects = [
        "/".join([s3_csv_handler._get_bucket()] + [s3_csv_handler._get_prefix()] + [f"{col}={val}" for col, val in
                                                                                    zip(partition_cols,
                                                                                        combination)] + [filename])
        for combination in all_combinations
    ]
    actual_objects = list_objects(bucket, prefix)
    assert sorted(expected_objects) == sorted(actual_objects)


@patch('common.handler.datetime')
def test_write_multi_level_partitioned_data_to_s3(mock_datetime, mock_boto, sample_data):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    filename = '20230101120000.csv'
    partition_cols = ["Region", "Salesperson"]
    s3_csv_handler = S3CSVHandler(bucket, prefix, partition_cols=partition_cols)
    s3_csv_handler.write(sample_data)
    all_combinations = set(tuple(entry[col] for col in partition_cols) for entry in sample_data)
    expected_objects = [
        "/".join([s3_csv_handler._get_bucket()] + [s3_csv_handler._get_prefix()] + [f"{col}={val}" for col, val in
                                                                                    zip(partition_cols,
                                                                                        combination)] + [filename])
        for combination in all_combinations
    ]
    actual_objects = list_objects(bucket, prefix)
    assert sorted(expected_objects) == sorted(actual_objects)


@pytest.mark.xfail(raises=KeyError)
def test_partition_key_not_found(mock_boto, sample_data):
    partition_cols = ["Region", "Salesperson"]
    s3_csv_handler = S3CSVHandler(bucket, prefix, partition_cols=partition_cols)
    s3_csv_handler.write(sample_data)


@pytest.mark.xfail(raises=ValueError)
def test_no_data(mock_boto):
    s3_csv_handler = S3CSVHandler(bucket, prefix)
    s3_csv_handler.write(None)
