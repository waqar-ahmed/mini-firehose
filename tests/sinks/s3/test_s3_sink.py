import os

import boto3
import pandas as pd
import pytest
import s3fs
from moto import mock_s3
from sinks.s3.s3_sink import S3Sink

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


def test_transformation(mock_boto, sample_data):
    def transformation_callback(df):
        df["new_column"] = 1
        return df

    filename = "test.csv"
    local_sink = S3Sink(bucket, prefix, 'csv', transformation_callback=transformation_callback)
    local_sink.deliver(sample_data, filename)

    expected_df = pd.DataFrame(sample_data)
    expected_df["new_column"] = 1

    s3_fs = s3fs.S3FileSystem()
    file_path = f"s3://{bucket}/{prefix}/{filename}"
    actual_df = pd.read_csv(s3_fs.open(file_path, mode="r"))

    assert expected_df.shape == actual_df.shape, "DataFrames have different shapes."
    pd.testing.assert_frame_equal(expected_df, actual_df)