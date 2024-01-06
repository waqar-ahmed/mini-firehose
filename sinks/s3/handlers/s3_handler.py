import boto3
import pandas as pd
import s3fs
from botocore.exceptions import ClientError

from common.handler import Handler
import logging


logger = logging.getLogger(__name__)


class S3Handler(Handler):
    def __init__(self, bucket, prefix, file_type, partition_cols=None, filename_based_on='datetime', s3_config=None):
        super().__init__(file_type, filename_based_on)
        self.bucket = "s3://" + bucket
        self.prefix = prefix
        self.partition_cols = partition_cols
        self.has_partitions = partition_cols is not None and len(partition_cols) > 0

        if s3_config is not None:
            self.s3_fs = s3fs.S3FileSystem(
                key=s3_config["access-key"],
                secret=s3_config["secret-key"],
                client_kwargs={'endpoint_url': s3_config["endpoint-url"]}
            )
        else:
            self.s3_fs = s3fs.S3FileSystem()

        sts = boto3.client('sts')
        try:
            sts.get_caller_identity()
            logger.info("Credentials are available and valid.")
        except ClientError:
            logger.info("Credentials are not available or valid. Initializing using config.")

    def _get_bucket(self):
        return self.bucket

    def _get_prefix(self):
        return self.prefix

    def get_file_path(self, filename=None):
        if not filename or filename == '':
            filename = self._generate_filename()
        return "/".join([self.bucket, self.prefix, filename])

    def get_partition_path(self, group_name):
        if not isinstance(group_name, tuple):
            group_name = (group_name,)
        folder_parts = [self.bucket] + [self.prefix] + [f"{col}={val}" for col, val in
                                                        zip(self.partition_cols, group_name)]
        partition_path = "/".join(folder_parts + [self._generate_filename()])
        return partition_path

    # It is abstract method
    def _write_data(self, df, file_path):
        raise NotImplementedError("This method should be implemented by subclasses")

    def write(self, df, filename=None):
        if self.has_partitions:
            for group_name, group_data in df.groupby(self.partition_cols):
                group_data = group_data.drop(columns=self.partition_cols)
                partition_file_path = self.get_partition_path(group_name)
                self._write_data(group_data, partition_file_path)
        else:
            file_path = self.get_file_path(filename)
            self._write_data(df, file_path)